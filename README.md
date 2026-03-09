# mtcp-rs

这是基于当前 Node 版 `Node-mTCP` 协议重写的 Rust 实现，放在独立目录里，方便和原项目并存。

## 兼容范围

- 保持和原版相同的握手格式与分帧格式。
- 提供两个入口：
  - `client`：本地 `tcp -> mtcp`
  - `remote`：远端 `mtcp -> tcp`
- 可以和现有 Node 版混合部署，只要两端都使用同一套协议。

## 构建

```bash
cargo build --release
```

## 运行

服务端：

```bash
cargo run --release --bin remote -- \
  --listen-port 15201 \
  --upstream-host 0.0.0.0 \
  --upstream-port 5201
```

客户端：

```bash
cargo run --release --bin client -- \
  --listen-port 5201 \
  --upstream-host 8.8.8.8 \
  --upstream-port 15201 \
  --pool-count 3 \
  --preconnect 10
```

多出口聚合时，用 `--upstream-hosts`：

```bash
cargo run --release --bin client -- \
  --listen-port 5201 \
  --upstream-hosts 1.1.1.1,2.2.2.2,3.3.3.3 \
  --upstream-port 15201
```

## 环境变量

两个二进制都支持环境变量覆盖配置：

- `MTCP_LISTEN_HOST`
- `MTCP_LISTEN_PORT`
- `MTCP_UPSTREAM_HOST`
- `MTCP_UPSTREAM_PORT`

客户端额外支持：

- `MTCP_UPSTREAM_HOSTS`
- `MTCP_POOL_COUNT`
- `MTCP_PRECONNECT`

## 和 Node 版的差异

- Rust 版把预连接池放在客户端侧使用，这和原始设计意图一致，也更符合 0-RTT 场景。
- 实现只依赖标准库，没有额外第三方 crate，便于直接编译。
