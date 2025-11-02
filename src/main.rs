mod sftp;
mod sshfs;
mod inode_map;

use nfsserve::tcp::*;
use sshfs::SshFS;
use sftp::{SFTPConnection, SFTPOpenFlags};

// Test with
// mount -t nfs -o nolocks,vers=3,tcp,port=12000,mountport=12000,soft 127.0.0.1:/ mnt/
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug")).init();

    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!("Usage: {} <hostname> <username> <remote_path>", args[0]);
        eprintln!("Example: {} example.com myuser /home/myuser", args[0]);
        std::process::exit(1);
    }

    let hostname = args[1].clone();
    let username = args[2].clone();
    let remote_path = args[3].clone();

    println!("Will connect to {}@{} serving {}", username, hostname, remote_path);

    //// Create and connect
    //let conn = SFTPConnection::new(hostname.to_string(), 22, username.to_string());
    //conn.connect().await?;

    //println!("Connected! Listing directory: {}", remote_path);
    //println!();

    //// List directory
    //match conn.list_directory(remote_path).await {
    //    Ok(entries) => {
    //        for entry in entries {
    //            // Skip . and ..
    //            if entry.filename == "." || entry.filename == ".." {
    //                continue;
    //            }

    //            let type_indicator = if entry.attributes.is_directory() {
    //                "/"
    //            } else {
    //                ""
    //            };

    //            println!(
    //                "{} {}{}",
    //                entry.attributes, entry.filename, type_indicator
    //            );
    //        }
    //    }
    //    Err(e) => {
    //        eprintln!("Error listing directory: {}", e);
    //    }
    //}

    //println!();
    ////println!("Testing file operations...");

    ////// Test file operations: write a test file
    ////let test_path = format!("{}/test_from_rust.txt", remote_path);
    ////let test_content = b"Hello from Rust SFTP!\n";

    ////println!("Writing to {}...", test_path);
    ////match conn
    ////    .open(
    ////        &test_path,
    ////        SFTPOpenFlags::WRITE | SFTPOpenFlags::CREATE | SFTPOpenFlags::TRUNCATE,
    ////    )
    ////    .await
    ////{
    ////    Ok(handle) => {
    ////        conn.write(&handle, 0, test_content).await?;
    ////        conn.close(&handle).await?;
    ////        println!("File written successfully!");

    ////        // Now read it back
    ////        println!("Reading back {}...", test_path);
    ////        let read_handle = conn.open(&test_path, SFTPOpenFlags::READ).await?;
    ////        let data = conn.read(&read_handle, 0, 1024).await?;
    ////        conn.close(&read_handle).await?;

    ////        println!("Read {} bytes:", data.len());
    ////        println!("{}", String::from_utf8_lossy(&data));

    ////        // Get file stats
    ////        let attrs = conn.stat(&test_path).await?;
    ////        println!("File attributes: {}", attrs);
    ////    }
    ////    Err(e) => {
    ////        eprintln!("Error writing file: {}", e);
    ////    }
    ////}

    //// Disconnect
    //println!();
    //println!("Disconnecting...");
    //conn.disconnect().await;

    let fs = SshFS::new(hostname, username, 22, remote_path);

    println!("Starting NFS server on 127.0.0.1:11111");
    println!("Mount with: sudo mount -t nfs -o nolocks,vers=3,tcp,port=11111,mountport=11111,soft 127.0.0.1:/ /path/to/mountpoint");

    let listener = NFSTcpListener::bind("127.0.0.1:11111", fs).await?;
    listener.handle_forever().await?;

    Ok(())
}
