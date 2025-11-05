#![allow(
dead_code)]

mod sftp;
mod sshfs;
mod inode_map;
mod handle_cache;

use nfsserve::tcp::*;
use sshfs::SshFS;
use std::env;

struct Config {
    username: String,
    hostname: String,
    remote_dir: String,
    mountpoint: String,
}

fn parse_args() -> Result<Config, String> {
    let args: Vec<String> = env::args().collect();

    if args.len() != 3 {
        return Err(format!(
            "Usage: {} [user@]host:[dir] mountpoint\n\
             \n\
             Examples:\n\
               {} user@host.com:/home/user /mnt/remote\n\
               {} host.com: /mnt/remote\n\
               {} host.com:/path /mnt/remote",
            args[0], args[0], args[0], args[0]
        ));
    }

    let source = &args[1];
    let mountpoint = args[2].clone();

    // Parse [user@]host:[dir]
    // Split on ':' first to separate host part from directory part
    let parts: Vec<&str> = source.splitn(2, ':').collect();
    if parts.len() != 2 {
        return Err(format!(
            "Invalid source format '{}'. Expected [user@]host:[dir]",
            source
        ));
    }

    let host_part = parts[0];
    let dir_part = parts[1];

    // Parse [user@]host
    let (username, hostname) = if host_part.contains('@') {
        let user_host: Vec<&str> = host_part.splitn(2, '@').collect();
        (user_host[0].to_string(), user_host[1].to_string())
    } else {
        // Use current user if not specified
        let current_user = env::var("USER")
            .or_else(|_| env::var("USERNAME"))
            .unwrap_or_else(|_| "root".to_string());
        (current_user, host_part.to_string())
    };

    // Use provided directory or default to /
    let remote_dir = if dir_part.is_empty() {
        "/".to_string()
    } else {
        dir_part.to_string()
    };

    Ok(Config {
        username,
        hostname,
        remote_dir,
        mountpoint,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    // Parse command line arguments
    let config = parse_args().map_err(|e| {
        eprintln!("{}", e);
        std::process::exit(1);
    })?;

    println!("Connecting to {}@{}:{}", config.username, config.hostname, config.remote_dir);

    let fs = SshFS::new(
        config.hostname.clone(),
        config.username.clone(),
        22,
        config.remote_dir.clone(),
    );

    let bind_addr = "127.0.0.1:11111";
    println!("Starting NFS server on {}", bind_addr);
    println!("Mount with: mount -t nfs -o nolocks,vers=3,tcp,port=11111,mountport=11111,soft 127.0.0.1:/ {}", config.mountpoint);

    let listener = NFSTcpListener::bind(bind_addr, fs).await?;
    listener.handle_forever().await?;

    Ok(())
}
