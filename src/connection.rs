use std::collections::HashMap;
use std::io::{self, ErrorKind, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use crate::protocol::{
    encode_frame, encode_login, next_pid, parse_login, try_parse_frame, HEADER_LEN,
    MAX_FRAME_PAYLOAD,
};

const WRITE_CHANNEL_DEPTH: usize = 64;

#[derive(Clone)]
pub struct MSocket {
    inner: Arc<Inner>,
}

struct Inner {
    state: Mutex<State>,
    read_cv: Condvar,
    closed_cv: Condvar,
}

struct State {
    mid: Option<u16>,
    read_pid: u16,
    write_pid: u16,
    packages: HashMap<u16, Vec<u8>>,
    read_cache: Vec<u8>,
    read_offset: usize,
    conns: Vec<Arc<SubConn>>,
    write_shutdown: bool,
    closed: bool,
}

struct SubConn {
    #[allow(dead_code)]
    cid: u16,
    sender: SyncSender<WriteMsg>,
    queued_bytes: AtomicUsize,
    alive: AtomicBool,
}

enum WriteMsg {
    Frame(Vec<u8>),
    Shutdown,
    Close,
}

impl SubConn {
    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }
}

impl MSocket {
    fn new(mid: Option<u16>) -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    mid,
                    read_pid: 0,
                    write_pid: 0,
                    packages: HashMap::new(),
                    read_cache: Vec::new(),
                    read_offset: 0,
                    conns: Vec::new(),
                    write_shutdown: false,
                    closed: false,
                }),
                read_cv: Condvar::new(),
                closed_cv: Condvar::new(),
            }),
        }
    }

    pub fn connect(hosts: &[String], port: u16, pool_count: usize) -> io::Result<Self> {
        if hosts.is_empty() {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "at least one upstream host is required",
            ));
        }

        let socket = Self::new(None);
        let total = if hosts.len() > 1 {
            hosts.len()
        } else {
            pool_count.max(1)
        };

        for index in 0..total {
            let host = if hosts.len() > 1 {
                &hosts[index]
            } else {
                &hosts[0]
            };

            let result = socket.connect_sub(host, port);
            if index == 0 {
                result?;
            } else if result.is_err() {
                continue;
            }

            if index + 1 < total {
                thread::sleep(Duration::from_millis(50));
            }
        }

        if socket.connection_count() == 0 {
            return Err(io::Error::new(
                ErrorKind::NotConnected,
                "failed to establish any mtcp sub-connections",
            ));
        }

        Ok(socket)
    }

    pub fn from_server(mid: u16) -> Self {
        Self::new(Some(mid))
    }

    pub fn mid(&self) -> Option<u16> {
        self.inner.state.lock().unwrap().mid
    }

    pub fn is_closed(&self) -> bool {
        self.inner.state.lock().unwrap().closed
    }

    pub fn wait_closed(&self) {
        let mut state = self.inner.state.lock().unwrap();
        while !state.closed {
            state = self.inner.closed_cv.wait(state).unwrap();
        }
    }

    pub fn shutdown_write(&self) -> io::Result<()> {
        let conns = {
            let mut state = self.inner.state.lock().unwrap();
            if state.write_shutdown {
                return Ok(());
            }
            state.write_shutdown = true;
            state
                .conns
                .iter()
                .filter(|conn| conn.is_alive())
                .cloned()
                .collect::<Vec<_>>()
        };

        for conn in conns {
            let _ = conn.sender.send(WriteMsg::Shutdown);
        }

        Ok(())
    }

    pub fn close(&self) {
        let conns = {
            let mut state = self.inner.state.lock().unwrap();
            state.write_shutdown = true;
            state
                .conns
                .iter()
                .filter(|conn| conn.is_alive())
                .cloned()
                .collect::<Vec<_>>()
        };

        for conn in conns {
            let _ = conn.sender.send(WriteMsg::Close);
        }
    }

    fn connection_count(&self) -> usize {
        self.inner
            .state
            .lock()
            .unwrap()
            .conns
            .iter()
            .filter(|conn| conn.is_alive())
            .count()
    }

    fn connect_sub(&self, host: &str, port: u16) -> io::Result<()> {
        let mut stream = TcpStream::connect((host, port))?;
        let _ = stream.set_nodelay(true);

        let mut cid_buf = [0u8; 2];
        stream.read_exact(&mut cid_buf)?;
        let cid = u16::from_be_bytes(cid_buf);

        let mid = {
            let mut state = self.inner.state.lock().unwrap();
            let mid = state.mid.get_or_insert(cid);
            *mid
        };

        stream.write_all(&encode_login(cid, mid))?;
        self.attach_stream(stream, cid)
    }

    pub(crate) fn attach_stream(&self, stream: TcpStream, cid: u16) -> io::Result<()> {
        let reader = stream.try_clone()?;
        let (sender, receiver) = sync_channel(WRITE_CHANNEL_DEPTH);
        let sub_conn = Arc::new(SubConn {
            cid,
            sender,
            queued_bytes: AtomicUsize::new(0),
            alive: AtomicBool::new(true),
        });

        {
            let mut state = self.inner.state.lock().unwrap();
            state.closed = false;
            state.conns.push(sub_conn.clone());
        }

        let read_socket = self.clone();
        let read_conn = sub_conn.clone();
        thread::spawn(move || reader_loop(reader, read_socket, read_conn));

        let write_socket = self.clone();
        thread::spawn(move || writer_loop(stream, write_socket, sub_conn, receiver));

        Ok(())
    }

    fn deliver_frame(&self, pid: u16, payload: Vec<u8>) {
        let mut state = self.inner.state.lock().unwrap();
        state.packages.entry(pid).or_insert(payload);
        self.inner.read_cv.notify_all();
    }

    fn mark_connection_closed(&self, conn: &Arc<SubConn>) {
        if !conn.alive.swap(false, Ordering::AcqRel) {
            return;
        }

        let mut state = self.inner.state.lock().unwrap();
        state.conns.retain(|candidate| candidate.is_alive());
        if state.conns.is_empty() {
            state.closed = true;
            self.inner.closed_cv.notify_all();
        }
        self.inner.read_cv.notify_all();
    }

    fn write_buffer(&self, mut bytes: &[u8]) -> io::Result<()> {
        while !bytes.is_empty() {
            let chunk_len = bytes.len().min(MAX_FRAME_PAYLOAD);
            let pid = {
                let mut state = self.inner.state.lock().unwrap();
                if state.write_shutdown {
                    return Err(io::Error::new(
                        ErrorKind::BrokenPipe,
                        "msocket write side is closed",
                    ));
                }
                let pid = state.write_pid;
                state.write_pid = next_pid(state.write_pid);
                pid
            };

            let frame = encode_frame(pid, &bytes[..chunk_len]);
            self.write_frame(frame)?;
            bytes = &bytes[chunk_len..];
        }

        Ok(())
    }

    fn write_frame(&self, frame: Vec<u8>) -> io::Result<()> {
        let mut msg = WriteMsg::Frame(frame);

        loop {
            let conn = {
                let mut state = self.inner.state.lock().unwrap();
                state.conns.retain(|candidate| candidate.is_alive());

                if state.write_shutdown {
                    return Err(io::Error::new(
                        ErrorKind::BrokenPipe,
                        "msocket write side is closed",
                    ));
                }

                let Some(conn) = state
                    .conns
                    .iter()
                    .filter(|candidate| candidate.is_alive())
                    .min_by_key(|candidate| candidate.queued_bytes.load(Ordering::Acquire))
                    .cloned()
                else {
                    return Err(io::Error::new(
                        ErrorKind::NotConnected,
                        "msocket has no active sub-connections",
                    ));
                };

                if let WriteMsg::Frame(ref frame) = msg {
                    conn.queued_bytes.fetch_add(frame.len(), Ordering::AcqRel);
                }

                conn
            };

            msg = match conn.sender.send(msg) {
                Ok(()) => return Ok(()),
                Err(err) => {
                    if let WriteMsg::Frame(ref frame) = err.0 {
                        conn.queued_bytes.fetch_sub(frame.len(), Ordering::AcqRel);
                    }
                    self.mark_connection_closed(&conn);
                    err.0
                }
            };
        }
    }
}

impl Read for MSocket {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let mut state = self.inner.state.lock().unwrap();
        loop {
            if state.read_offset < state.read_cache.len() {
                let remaining = state.read_cache.len() - state.read_offset;
                let copy_len = remaining.min(buf.len());
                buf[..copy_len].copy_from_slice(
                    &state.read_cache[state.read_offset..state.read_offset + copy_len],
                );
                state.read_offset += copy_len;
                if state.read_offset == state.read_cache.len() {
                    state.read_cache.clear();
                    state.read_offset = 0;
                }
                return Ok(copy_len);
            }

            let read_pid = state.read_pid;
            if let Some(payload) = state.packages.remove(&read_pid) {
                state.read_pid = next_pid(state.read_pid);
                state.read_cache = payload;
                state.read_offset = 0;
                continue;
            }

            state.conns.retain(|conn| conn.is_alive());
            if state.conns.is_empty() {
                state.closed = true;
                self.inner.closed_cv.notify_all();
                return Ok(0);
            }

            state = self.inner.read_cv.wait(state).unwrap();
        }
    }
}

impl Write for MSocket {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        self.write_buffer(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn reader_loop(mut stream: TcpStream, socket: MSocket, conn: Arc<SubConn>) {
    let mut buffered = Vec::with_capacity(16 * 1024);
    let mut read_buf = [0u8; 16 * 1024];

    loop {
        let bytes_read = match stream.read(&mut read_buf) {
            Ok(0) => break,
            Ok(bytes_read) => bytes_read,
            Err(err) if err.kind() == ErrorKind::Interrupted => continue,
            Err(_) => break,
        };

        buffered.extend_from_slice(&read_buf[..bytes_read]);

        let mut offset = 0usize;
        while let Some((frame_len, pid, payload_len)) = try_parse_frame(&buffered[offset..]) {
            let payload_start = offset + HEADER_LEN;
            let payload_end = payload_start + payload_len;
            socket.deliver_frame(pid, buffered[payload_start..payload_end].to_vec());
            offset += frame_len;
        }

        if offset > 0 {
            buffered.drain(..offset);
        }
    }

    socket.mark_connection_closed(&conn);
}

fn writer_loop(
    mut stream: TcpStream,
    socket: MSocket,
    conn: Arc<SubConn>,
    receiver: Receiver<WriteMsg>,
) {
    while let Ok(msg) = receiver.recv() {
        match msg {
            WriteMsg::Frame(frame) => {
                let frame_len = frame.len();
                let result = stream.write_all(&frame);
                conn.queued_bytes.fetch_sub(frame_len, Ordering::AcqRel);
                if result.is_err() {
                    break;
                }
            }
            WriteMsg::Shutdown => {
                let _ = stream.shutdown(Shutdown::Write);
            }
            WriteMsg::Close => {
                let _ = stream.shutdown(Shutdown::Both);
                break;
            }
        }
    }

    socket.mark_connection_closed(&conn);
}

struct ServerState {
    next_cid: Mutex<u16>,
    sockets: Mutex<HashMap<u16, MSocket>>,
}

impl Default for ServerState {
    fn default() -> Self {
        Self {
            next_cid: Mutex::new(1),
            sockets: Mutex::new(HashMap::new()),
        }
    }
}

impl ServerState {
    fn allocate_cid(&self) -> u16 {
        let active = self.sockets.lock().unwrap();
        let mut next = self.next_cid.lock().unwrap();

        loop {
            if *next == 0 || *next >= 65_000 {
                *next = 1;
            }

            let cid = *next;
            *next = next_pid(*next);
            if cid != 0 && !active.contains_key(&cid) {
                return cid;
            }
        }
    }
}

pub fn run_mtcp_server<F>(listen_addr: &str, handler: F) -> io::Result<()>
where
    F: Fn(MSocket) + Send + Sync + 'static,
{
    let listener = TcpListener::bind(listen_addr)?;
    serve_mtcp_listener(listener, handler)
}

pub fn serve_mtcp_listener<F>(listener: TcpListener, handler: F) -> io::Result<()>
where
    F: Fn(MSocket) + Send + Sync + 'static,
{
    let state = Arc::new(ServerState::default());
    let handler = Arc::new(handler);

    for incoming in listener.incoming() {
        let stream = match incoming {
            Ok(stream) => stream,
            Err(err) => {
                eprintln!("mtcp accept failed: {err}");
                continue;
            }
        };

        let state = state.clone();
        let handler = handler.clone();
        thread::spawn(move || {
            if let Err(err) = handle_server_stream(stream, state, handler) {
                eprintln!("mtcp handshake failed: {err}");
            }
        });
    }

    Ok(())
}

fn handle_server_stream<F>(
    mut stream: TcpStream,
    state: Arc<ServerState>,
    handler: Arc<F>,
) -> io::Result<()>
where
    F: Fn(MSocket) + Send + Sync + 'static,
{
    let _ = stream.set_nodelay(true);

    let cid = state.allocate_cid();
    stream.write_all(&cid.to_be_bytes())?;

    let mut login = [0u8; 4];
    stream.read_exact(&mut login)?;
    let mid = parse_login(login, cid)?;

    let mut created = false;
    let socket = {
        let mut sockets = state.sockets.lock().unwrap();
        if let Some(socket) = sockets.get(&mid).cloned() {
            socket
        } else {
            created = true;
            let socket = MSocket::from_server(mid);
            sockets.insert(mid, socket.clone());
            socket
        }
    };

    if let Err(err) = socket.attach_stream(stream, cid) {
        if created {
            state.sockets.lock().unwrap().remove(&mid);
        }
        return Err(err);
    }

    if created {
        let cleanup_socket = socket.clone();
        let cleanup_state = state.clone();
        thread::spawn(move || {
            cleanup_socket.wait_closed();
            cleanup_state.sockets.lock().unwrap().remove(&mid);
        });

        thread::spawn(move || handler(socket));
    }

    Ok(())
}
