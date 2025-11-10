# sshfs

A Rust implementation that exposes remote filesystems over SSH/SFTP as a local NFS mount.

## Status

**Active Development/ready for experimental use** - This project is currently under active development. It can mount remote systems as read-only or read-write. Risk of data loss is low but user be warned.

## What is it?

Think of it like glue/middleware between an NFS server and SFTP. sshfs provides an NFS server that proxies filesystem operations to a remote system over SSH/SFTP. This allows you to mount remote directories using standard NFS clients without requiring FUSE or kernel extensions (particularly on macOS).

Just like the FUSE sshfs, this binary uses any/all SSH configuration you have
set up on your system.

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
4. Cleanup:
```bash
umount /path/to/mountpoint
### Safe to kill `sshfs`
killall sshfs
```

## TODOs

- Better configuration (CLI arguments, config file)
- Multiple simultaneous mounts
- Performance optimizations - write throughput is about 70-80% of sshfs+FUSE on
  Linux. Some of it is fundamental to the overhead of the NFS translation but I
  still want to do better.

## Requirements

- Rust 2024 edition
- SSH access to the remote system
- NFS client support on your local system

## License

See LICENSE file for details.
