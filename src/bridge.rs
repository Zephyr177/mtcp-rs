use std::io;
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::connection::{serve_mtcp_listener, MSocket};

#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub listen_host: String,
    pub listen_port: u16,
    pub upstream_hosts: Vec<String>,
    pub upstream_port: u16,
    pub pool_count: usize,
    pub preconnect: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            listen_host: "0.0.0.0".to_string(),
            listen_port: 5201,
            upstream_hosts: vec!["8.8.8.8".to_string()],
            upstream_port: 15201,
            pool_count: 3,
            preconnect: 10,
        }
    }
}

impl ClientConfig {
    fn listen_addr(&self) -> String {
        format!("{}:{}", self.listen_host, self.listen_port)
    }

    fn upstream_label(&self) -> String {
        self.upstream_hosts.join(",")
    }
}

#[derive(Clone, Debug)]
pub struct RemoteConfig {
    pub listen_host: String,
    pub listen_port: u16,
    pub upstream_host: String,
    pub upstream_port: u16,
}

impl Default for RemoteConfig {
    fn default() -> Self {
        Self {
            listen_host: "0.0.0.0".to_string(),
            listen_port: 15201,
            upstream_host: "0.0.0.0".to_string(),
            upstream_port: 5201,
        }
    }
}

impl RemoteConfig {
    fn listen_addr(&self) -> String {
        format!("{}:{}", self.listen_host, self.listen_port)
    }
}

pub fn run_client_bridge(config: ClientConfig) -> io::Result<()> {
    let listener = TcpListener::bind(config.listen_addr())?;
    let pool = PreconnectPool::new(config.clone());
    pool.start();

    println!(
        "启动成功:listen tcp:{} -> mtcp:{}:{}",
        config.listen_port,
        config.upstream_label(),
        config.upstream_port
    );

    for incoming in listener.incoming() {
        let stream = match incoming {
            Ok(stream) => stream,
            Err(err) => {
                eprintln!("client accept failed: {err}");
                continue;
            }
        };

        let pool = pool.clone();
        thread::spawn(move || {
            let socket = match pool.take() {
                Ok(socket) => socket,
                Err(err) => {
                    eprintln!("mtcp upstream connect failed: {err}");
                    return;
                }
            };

            if let Err(err) = bridge_tcp_and_mtcp(stream, socket) {
                eprintln!("client bridge failed: {err}");
            }
        });
    }

    Ok(())
}

pub fn run_remote_bridge(config: RemoteConfig) -> io::Result<()> {
    let listener = TcpListener::bind(config.listen_addr())?;
    println!(
        "启动成功:listen mtcp:{} -> tcp:{}:{}",
        config.listen_port, config.upstream_host, config.upstream_port
    );

    serve_mtcp_listener(listener, move |socket| {
        let stream = match TcpStream::connect((config.upstream_host.as_str(), config.upstream_port))
        {
            Ok(stream) => stream,
            Err(err) => {
                eprintln!("remote upstream connect failed: {err}");
                socket.close();
                return;
            }
        };

        if let Err(err) = bridge_tcp_and_mtcp(stream, socket) {
            eprintln!("remote bridge failed: {err}");
        }
    })
}

struct PreconnectPool {
    config: ClientConfig,
    sockets: Mutex<Vec<MSocket>>,
}

impl PreconnectPool {
    fn new(config: ClientConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            sockets: Mutex::new(Vec::new()),
        })
    }

    fn start(self: &Arc<Self>) {
        if self.config.preconnect == 0 {
            return;
        }

        let pool = self.clone();
        thread::spawn(move || pool.fill_loop());
    }

    fn fill_loop(self: Arc<Self>) {
        loop {
            {
                let mut sockets = self.sockets.lock().unwrap();
                sockets.retain(|socket| !socket.is_closed());
                if sockets.len() >= self.config.preconnect {
                    drop(sockets);
                    thread::sleep(Duration::from_millis(250));
                    continue;
                }
            }

            match MSocket::connect(
                &self.config.upstream_hosts,
                self.config.upstream_port,
                self.config.pool_count,
            ) {
                Ok(socket) => {
                    self.sockets.lock().unwrap().push(socket);
                }
                Err(err) => {
                    eprintln!("preconnect failed: {err}");
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }
    }

    fn take(&self) -> io::Result<MSocket> {
        {
            let mut sockets = self.sockets.lock().unwrap();
            while let Some(socket) = sockets.pop() {
                if !socket.is_closed() {
                    return Ok(socket);
                }
            }
        }

        MSocket::connect(
            &self.config.upstream_hosts,
            self.config.upstream_port,
            self.config.pool_count,
        )
    }
}

fn bridge_tcp_and_mtcp(stream: TcpStream, socket: MSocket) -> io::Result<()> {
    let _ = stream.set_nodelay(true);

    let mut tcp_reader = stream.try_clone()?;
    let mut tcp_writer = stream;
    let mut mtcp_writer = socket.clone();
    let mut mtcp_reader = socket.clone();

    let forward = thread::spawn(move || -> io::Result<()> {
        let result = io::copy(&mut tcp_reader, &mut mtcp_writer);
        let _ = mtcp_writer.shutdown_write();
        result.map(|_| ())
    });

    let backward = thread::spawn(move || -> io::Result<()> {
        let result = io::copy(&mut mtcp_reader, &mut tcp_writer);
        let _ = tcp_writer.shutdown(Shutdown::Write);
        result.map(|_| ())
    });

    let forward_result = join_copy_thread(forward, "tcp -> mtcp")?;
    let backward_result = join_copy_thread(backward, "mtcp -> tcp")?;

    if forward_result.is_err() {
        socket.close();
        return forward_result;
    }

    if backward_result.is_err() {
        socket.close();
        return backward_result;
    }

    Ok(())
}

fn join_copy_thread(
    handle: thread::JoinHandle<io::Result<()>>,
    label: &str,
) -> io::Result<io::Result<()>> {
    handle
        .join()
        .map_err(|_| io::Error::other(format!("bridge thread panicked while copying {label}")))
}
