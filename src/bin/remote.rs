use std::env;
use std::process;

use mtcp_rs::{run_remote_bridge, RemoteConfig};

const USAGE: &str = "\
Usage: remote [options]

Options:
  --listen-host <host>
  --listen-port <port>
  --upstream-host <host>
  --upstream-port <port>

Env overrides:
  MTCP_LISTEN_HOST
  MTCP_LISTEN_PORT
  MTCP_UPSTREAM_HOST
  MTCP_UPSTREAM_PORT
";

fn main() {
    let config = match load_config() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("{err}");
            process::exit(2);
        }
    };

    if let Err(err) = run_remote_bridge(config) {
        eprintln!("remote exited with error: {err}");
        process::exit(1);
    }
}

fn load_config() -> Result<RemoteConfig, String> {
    let mut config = RemoteConfig::default();

    if let Ok(value) = env::var("MTCP_LISTEN_HOST") {
        config.listen_host = value;
    }
    if let Ok(value) = env::var("MTCP_LISTEN_PORT") {
        config.listen_port = parse_u16("MTCP_LISTEN_PORT", &value)?;
    }
    if let Ok(value) = env::var("MTCP_UPSTREAM_HOST") {
        config.upstream_host = value;
    }
    if let Ok(value) = env::var("MTCP_UPSTREAM_PORT") {
        config.upstream_port = parse_u16("MTCP_UPSTREAM_PORT", &value)?;
    }

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen-host" => config.listen_host = next_value(&mut args, &arg)?,
            "--listen-port" => config.listen_port = parse_u16(&arg, &next_value(&mut args, &arg)?)?,
            "--upstream-host" => config.upstream_host = next_value(&mut args, &arg)?,
            "--upstream-port" => {
                config.upstream_port = parse_u16(&arg, &next_value(&mut args, &arg)?)?
            }
            "--help" | "-h" => return Err(USAGE.to_string()),
            _ => return Err(format!("unknown argument: {arg}\n\n{USAGE}")),
        }
    }

    Ok(config)
}

fn next_value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("missing value for {flag}\n\n{USAGE}"))
}

fn parse_u16(label: &str, value: &str) -> Result<u16, String> {
    value
        .parse::<u16>()
        .map_err(|_| format!("invalid value for {label}: {value}"))
}
