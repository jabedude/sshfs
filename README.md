# sshfs

A Rust implementation that exposes remote filesystems over SSH/SFTP as a local NFS mount.

## Status

**Active Development** - This project is currently under active development. It can mount remote systems as read-only or read-write.

## What is it?

Think of it like glue/middleware between an NFS server and SFTP. sshfs provides an NFS server that proxies filesystem operations to a remote system over SSH/SFTP. This allows you to mount remote directories using standard NFS clients without requiring FUSE or kernel extensions (particularly on macOS).

## Usage

1. Build the project:
```bash
cargo build --release
```

2. Run the server (mainly the same usage as FUSE sshfs):
```bash
./target/debug/sshfs [user@]host:[dir] mountpoint
```

3. Mount the proxied filesystem:
```bash
mount -t nfs -o nolocks,vers=3,tcp,port=11111,mountport=11111,soft 127.0.0.1:/ /path/to/mountpoint
```

## Current Features

- SSH/SFTP connection to remote systems
- NFS v3 server implementation
- Read/write filesystem operations
- Directory listing
- File reading

## Roadmap

- Better configuration (CLI arguments, config file)
- Multiple simultaneous mounts
- Performance optimizations

## Requirements

- Rust 2024 edition
- SSH access to the remote system
- NFS client support on your local system

## License

See LICENSE file for details.
