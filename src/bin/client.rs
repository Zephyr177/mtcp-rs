use std::env;
use std::process;

use mtcp_rs::{run_client_bridge, ClientConfig};

const USAGE: &str = "\
Usage: client [options]

Options:
  --listen-host <host>
  --listen-port <port>
  --upstream-host <host>
  --upstream-hosts <host1,host2,...>
  --upstream-port <port>
  --pool-count <count>
  --preconnect <count>

Env overrides:
  MTCP_LISTEN_HOST
  MTCP_LISTEN_PORT
  MTCP_UPSTREAM_HOST
  MTCP_UPSTREAM_HOSTS
  MTCP_UPSTREAM_PORT
  MTCP_POOL_COUNT
  MTCP_PRECONNECT
";

fn main() {
    let config = match load_config() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("{err}");
            process::exit(2);
        }
    };

    if let Err(err) = run_client_bridge(config) {
        eprintln!("client exited with error: {err}");
        process::exit(1);
    }
}

fn load_config() -> Result<ClientConfig, String> {
    let mut config = ClientConfig::default();

    if let Ok(value) = env::var("MTCP_LISTEN_HOST") {
        config.listen_host = value;
    }
    if let Ok(value) = env::var("MTCP_LISTEN_PORT") {
        config.listen_port = parse_u16("MTCP_LISTEN_PORT", &value)?;
    }
    if let Ok(value) = env::var("MTCP_UPSTREAM_HOSTS") {
        config.upstream_hosts = split_hosts(&value);
    } else if let Ok(value) = env::var("MTCP_UPSTREAM_HOST") {
        config.upstream_hosts = vec![value];
    }
    if let Ok(value) = env::var("MTCP_UPSTREAM_PORT") {
        config.upstream_port = parse_u16("MTCP_UPSTREAM_PORT", &value)?;
    }
    if let Ok(value) = env::var("MTCP_POOL_COUNT") {
        config.pool_count = parse_usize("MTCP_POOL_COUNT", &value)?;
    }
    if let Ok(value) = env::var("MTCP_PRECONNECT") {
        config.preconnect = parse_usize("MTCP_PRECONNECT", &value)?;
    }

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen-host" => config.listen_host = next_value(&mut args, &arg)?,
            "--listen-port" => config.listen_port = parse_u16(&arg, &next_value(&mut args, &arg)?)?,
            "--upstream-host" => config.upstream_hosts = vec![next_value(&mut args, &arg)?],
            "--upstream-hosts" => {
                config.upstream_hosts = split_hosts(&next_value(&mut args, &arg)?)
            }
            "--upstream-port" => {
                config.upstream_port = parse_u16(&arg, &next_value(&mut args, &arg)?)?
            }
            "--pool-count" => config.pool_count = parse_usize(&arg, &next_value(&mut args, &arg)?)?,
            "--preconnect" => config.preconnect = parse_usize(&arg, &next_value(&mut args, &arg)?)?,
            "--help" | "-h" => return Err(USAGE.to_string()),
            _ => return Err(format!("unknown argument: {arg}\n\n{USAGE}")),
        }
    }

    if config.upstream_hosts.is_empty() {
        return Err(format!("at least one upstream host is required\n\n{USAGE}"));
    }

    Ok(config)
}

fn next_value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("missing value for {flag}\n\n{USAGE}"))
}

fn split_hosts(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|host| !host.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn parse_u16(label: &str, value: &str) -> Result<u16, String> {
    value
        .parse::<u16>()
        .map_err(|_| format!("invalid value for {label}: {value}"))
}

fn parse_usize(label: &str, value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|_| format!("invalid value for {label}: {value}"))
}
