use crate::inode_map::InodeMap;
use crate::sftp::{SFTPConnection, SFTPOpenFlags};

use async_trait::async_trait;
use libc;
use log::{debug, error, info, warn};
use nfsserve::{
    nfs::{
        self, fattr3, fileid3, filename3, ftype3, nfspath3, nfsstat3, nfstime3, sattr3, specdata3,
    },
    tcp::*,
    vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities},
};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct SshFS {
    // Connection configuration
    hostname: String,
    username: String,
    port: u16,
    remote_root: String,

    // SFTP connection (lazy initialized)
    connection: Arc<Mutex<Option<SFTPConnection>>>,

    // Inode mapping
    inode_map: Arc<InodeMap>,
}

unsafe impl Sync for SshFS {}
unsafe impl Send for SshFS {}

impl SshFS {
    /// Create a new SSHFS instance
    ///
    /// # Arguments
    /// * `hostname` - SSH server hostname
    /// * `username` - SSH username
    /// * `port` - SSH port (usually 22)
    /// * `remote_root` - Remote path to treat as root (e.g., "/home/user")
    pub fn new(hostname: String, username: String, port: u16, remote_root: String) -> Self {
        Self {
            hostname,
            username,
            port,
            remote_root: remote_root.clone(),
            connection: Arc::new(Mutex::new(None)),
            inode_map: Arc::new(InodeMap::new(remote_root, 0)),
        }
    }

    /// Ensure the SFTP connection is established
    ///
    /// This is called lazily on the first operation. If the connection
    /// already exists, this is a no-op.
    async fn ensure_connected(&self) -> Result<(), nfsstat3> {
        let mut conn_guard = self.connection.lock().await;

        if conn_guard.is_none() {
            log::info!(
                "Establishing SFTP connection to {}@{}:{}",
                self.username,
                self.hostname,
                self.port
            );

            let sftp = SFTPConnection::new(
                self.hostname.clone(),
                self.port,
                self.username.clone(),
            );

            sftp.connect().await.map_err(|e| {
                log::error!("Failed to connect to SFTP server: {}", e);
                nfsstat3::NFS3ERR_IO
            })?;

            log::info!("SFTP connection established");
            *conn_guard = Some(sftp);
        }

        Ok(())
    }

    /// Execute an operation with the SFTP connection (closure-based)
    ///
    /// This automatically ensures the connection is established before
    /// calling the provided closure. This is the recommended way to
    /// interact with the SFTP connection.
    ///
    /// # Example
    /// ```
    /// self.with_connection(|sftp| async move {
    ///     sftp.stat(&path).await
    /// }).await
    /// ```
    async fn with_connection<F, Fut, T>(&self, f: F) -> Result<T, nfsstat3>
    where
        F: FnOnce(&SFTPConnection) -> Fut,
        Fut: std::future::Future<Output = Result<T, crate::sftp::SFTPError>>,
    {
        self.ensure_connected().await?;

        let conn_guard = self.connection.lock().await;
        let sftp = conn_guard.as_ref().unwrap();

        f(sftp).await.map_err(|e| {
            log::error!("SFTP operation failed: {}", e);
            // TODO: Map specific SFTP errors to NFS errors
            nfsstat3::NFS3ERR_IO
        })
    }

    /// Get the SFTP connection, ensuring it's established (guard-based)
    ///
    /// Alternative to with_connection() that returns a lock guard.
    /// The connection remains locked until the guard is dropped.
    ///
    /// # Example
    /// ```
    /// let sftp = self.sftp().await?;
    /// let attrs = sftp.stat(&path).await.map_err(Self::map_sftp_error)?;
    /// ```
    async fn sftp(&self) -> Result<impl std::ops::Deref<Target = SFTPConnection> + '_, nfsstat3> {
        self.ensure_connected().await?;

        let guard = self.connection.lock().await;

        // Map the MutexGuard<Option<SFTPConnection>> to just the SFTPConnection
        Ok(tokio::sync::MutexGuard::map(guard, |opt| {
            opt.as_mut().expect("Connection should be established")
        }))
    }

    /// Map errnos to NFS errors
    fn map_errno_error(code: u32) -> nfsstat3 {
        match code as i32 {
            libc::ENOENT => return nfsstat3::NFS3ERR_NOENT,
            _ => {}
        }
        nfsstat3::NFS3ERR_IO
    }

    /// Map SFTP errors to NFS errors
    fn map_sftp_error(e: crate::sftp::SFTPError) -> nfsstat3 {
        // TODO: More sophisticated error mapping
        log::error!("SFTP error: {}", e);
        match e {
            crate::sftp::SFTPError::ServerError(code, message) => {
                return Self::map_errno_error(code);
            },
            _ => {}
        }

        nfsstat3::NFS3ERR_IO
    }

    /// Convert SFTP FileAttributes to NFS fattr3
    fn sftp_attrs_to_nfs(&self, attrs: crate::sftp::FileAttributes, fileid: fileid3) -> fattr3 {
        fattr3 {
            ftype: Self::extract_file_type(attrs.permissions),
            mode: attrs.permissions & 0o7777,  // Permission bits only
            nlink: 1,  // FIXME: SFTP doesn't seem to provide hard link count
            uid: attrs.uid,
            gid: attrs.gid,
            size: attrs.size,
            used: attrs.size,  // FIXME: Approximation - SFTP doesn't seem to provide actual disk usage
            rdev: specdata3 {
                specdata1: 0,
                specdata2: 0,
            },
            fsid: 0,  // FIXME: Filesystem ID - using 0 as we only have one filesystem
            fileid,
            atime: Self::systemtime_to_nfstime(attrs.access_time),
            mtime: Self::systemtime_to_nfstime(attrs.modification_time),
            ctime: Self::systemtime_to_nfstime(attrs.modification_time),  // Use mtime for ctime
        }
    }

    /// Extract NFS file type from Unix permission bits
    fn extract_file_type(permissions: u32) -> ftype3 {
        match permissions & 0xF000 {  // S_IFMT mask
            0x4000 => ftype3::NF3DIR,   // S_IFDIR - directory
            0x8000 => ftype3::NF3REG,   // S_IFREG - regular file
            0xA000 => ftype3::NF3LNK,   // S_IFLNK - symbolic link
            0x6000 => ftype3::NF3BLK,   // S_IFBLK - block device
            0x2000 => ftype3::NF3CHR,   // S_IFCHR - character device
            0xC000 => ftype3::NF3SOCK,  // S_IFSOCK - socket
            0x1000 => ftype3::NF3FIFO,  // S_IFIFO - named pipe
            _ => ftype3::NF3REG,        // Default to regular file
        }
    }

    /// Convert SystemTime to NFS time format
    fn systemtime_to_nfstime(time: std::time::SystemTime) -> nfstime3 {
        let duration = time
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();

        nfstime3 {
            seconds: duration.as_secs() as u32,
            nseconds: duration.subsec_nanos(),
        }
    }
}

#[async_trait]
impl NFSFileSystem for SshFS {
    #[doc = " Returns the set of capabilities supported"]
    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadOnly
    }

    #[doc = " Returns the ID the of the root directory \"/\""]
    fn root_dir(&self) -> fileid3 {
        0
    }

    #[doc = " Look up the id of a path in a directory"]
    #[doc = ""]
    #[doc = " i.e. given a directory dir/ containing a file a.txt"]
    #[doc = " this may call lookup(id_of(\"dir/\"), \"a.txt\")"]
    #[doc = " and this should return the id of the file \"dir/a.txt\""]
    #[doc = ""]
    #[doc = " This method should be fast as it is used very frequently."]
    #[must_use]
    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        debug!("lookup: dirid: {dirid}, filename: {:?}", filename);
        let parent_path = self.inode_map.get_path(dirid).ok_or(nfsstat3::NFS3ERR_NOENT)?;
        let filename_str = String::from_utf8_lossy(filename.as_ref());
        let child_path = format!("{}/{}", parent_path.trim_end_matches('/'), filename_str);

        // Check if it exists via lstat
        let sftp = self.sftp().await?;
        // TODO: Think about if we can cache this to avoid two lstat's
        debug!("Looking up {child_path}");
        let _attrs = sftp.lstat(&child_path).await.map_err(Self::map_sftp_error)?;

        // It exists, allocate inode
        let inode = self.inode_map.get_or_allocate(&child_path);
        Ok(inode)
    }

    #[doc = " Returns the attributes of an id."]
    #[doc = " This method should be fast as it is used very frequently."]
    #[must_use]
    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        // Resolve inode to path
        let path = self.inode_map.get_path(id).ok_or_else(|| {
            log::warn!("getattr: unknown inode {}", id);
            nfsstat3::NFS3ERR_NOENT
        })?;

        log::debug!("getattr: id={} path={}", id, path);

        // Get SFTP file attributes (use lstat to not follow symlinks)
        let sftp = self.sftp().await?;
        let attrs = sftp.lstat(&path).await.map_err(Self::map_sftp_error)?;

        // Convert to NFS attributes
        let nfs_attrs = self.sftp_attrs_to_nfs(attrs, id);

        log::debug!("getattr: returning attrs for {}: size={}", path, nfs_attrs.size);
        Ok(nfs_attrs)
    }

    #[doc = " Sets the attributes of an id"]
    #[doc = " this should return Err(nfsstat3::NFS3ERR_ROFS) if readonly"]
    #[must_use]
    async fn setattr(&self, id: fileid3, setattr: sattr3) -> Result<fattr3, nfsstat3> {
        todo!()
    }

    #[doc = " Reads the contents of a file returning (bytes, EOF)"]
    #[doc = " Note that offset/count may go past the end of the file and that"]
    #[doc = " in that case, all bytes till the end of file are returned."]
    #[doc = " EOF must be flagged if the end of the file is reached by the read."]
    #[must_use]
    async fn read(
        &self,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        info!("read-ing {id} @{offset} for {count}");

        let path = self.inode_map.get_path(id).ok_or(nfsstat3::NFS3ERR_NOENT)?;

        let sftp = self.sftp().await?;
        let handle = sftp.open(&path, SFTPOpenFlags::READ).await.map_err(Self::map_sftp_error)?;
        let contents = sftp.read(&handle, offset, count).await.map_err(Self::map_sftp_error)?;
        sftp.close(handle).await.map_err(Self::map_sftp_error)?;

        // EOF if we got less data than requested OR got empty response
        let eof = contents.len() < count as usize;

        Ok((contents, eof))
    }

    #[doc = " Writes the contents of a file returning (bytes, EOF)"]
    #[doc = " Note that offset/count may go past the end of the file and that"]
    #[doc = " in that case, the file is extended."]
    #[doc = " If not supported due to readonly file system"]
    #[doc = " this should return Err(nfsstat3::NFS3ERR_ROFS)"]
    #[must_use]
    async fn write(&self, id: fileid3, offset: u64, data: &[u8]) -> Result<fattr3, nfsstat3> {
        todo!()
    }

    #[doc = " Creates a file with the following attributes."]
    #[doc = " If not supported due to readonly file system"]
    #[doc = " this should return Err(nfsstat3::NFS3ERR_ROFS)"]
    #[must_use]
    async fn create(
        &self,
        dirid: fileid3,
        filename: &filename3,
        setattr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        todo!()
    }

    #[doc = " Creates a file if it does not already exist"]
    #[doc = " this should return Err(nfsstat3::NFS3ERR_ROFS)"]
    #[must_use]
    async fn create_exclusive(
        &self,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        todo!()
    }

    #[doc = " Makes a directory with the following attributes."]
    #[doc = " If not supported dur to readonly file system"]
    #[doc = " this should return Err(nfsstat3::NFS3ERR_ROFS)"]
    #[must_use]
    async fn mkdir(
        &self,
        dirid: fileid3,
        dirname: &filename3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    #[doc = " Removes a file."]
    #[doc = " If not supported due to readonly file system"]
    #[doc = " this should return Err(nfsstat3::NFS3ERR_ROFS)"]
    #[must_use]
    async fn remove(&self, dirid: fileid3, filename: &filename3) -> Result<(), nfsstat3> {
        todo!()
    }

    #[doc = " Removes a file."]
    #[doc = " If not supported due to readonly file system"]
    #[doc = " this should return Err(nfsstat3::NFS3ERR_ROFS)"]
    #[must_use]
    async fn rename(
        &self,
        from_dirid: fileid3,
        from_filename: &filename3,
        to_dirid: fileid3,
        to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        todo!()
    }

    #[doc = " Returns the contents of a directory with pagination."]
    #[doc = " Directory listing should be deterministic."]
    #[doc = " Up to max_entries may be returned, and start_after is used"]
    #[doc = " to determine where to start returning entries from."]
    #[doc = ""]
    #[doc = " For instance if the directory has entry with ids [1,6,2,11,8,9]"]
    #[doc = " and start_after=6, readdir should returning 2,11,8,..."]
    #[must_use]
    async fn readdir(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        log::debug!("readdir id {dirid} with start after {start_after} and max {max_entries}");

        let dir_path = self.inode_map.get_path(dirid).ok_or(nfsstat3::NFS3ERR_NOENT)?;
        let sftp = self.sftp().await?;

        log::debug!("readdir-ing {}", dir_path);
        let sftp_entries = sftp.list_directory(&dir_path).await.map_err(Self::map_sftp_error)?;
        log::debug!("readdir {}: got {} entries", dir_path, sftp_entries.len());

        // Convert SFTP entries to NFS entries
        let mut nfs_entries = Vec::new();
        for entry in sftp_entries {
            // Skip . and .. - NFS handles these specially
            if entry.filename == "." || entry.filename == ".." {
                continue;
            }

            // Build full path for this entry
            let child_path = format!("{}/{}", dir_path.trim_end_matches('/'), entry.filename);

            // Allocate inode for this entry
            let inode = self.inode_map.get_or_allocate(&child_path);

            // Convert to NFS DirEntry
            nfs_entries.push(DirEntry {
                fileid: inode,
                name: entry.filename.into_bytes().into(),
                attr: self.sftp_attrs_to_nfs(entry.attributes, inode),
            });
        }

        // Apply pagination: skip entries <= start_after, take up to max_entries
        let filtered: Vec<DirEntry> = nfs_entries
            .into_iter()
            .filter(|e| e.fileid > start_after)
            .take(max_entries)
            .collect();

        // Determine if we've reached the end
        let end = filtered.len() < max_entries;

        log::debug!(
            "readdir: returning {} entries, end={}",
            filtered.len(),
            end
        );

        Ok(ReadDirResult {
            entries: filtered,
            end,
        })
    }

    #[doc = " Makes a symlink with the following attributes."]
    #[doc = " If not supported due to readonly file system"]
    #[doc = " this should return Err(nfsstat3::NFS3ERR_ROFS)"]
    #[must_use]
    async fn symlink(
        &self,
        dirid: fileid3,
        linkname: &filename3,
        symlink: &nfspath3,
        attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        todo!()
    }

    #[doc = " Reads a symlink"]
    #[must_use]
    async fn readlink(&self, id: fileid3) -> Result<nfspath3, nfsstat3> {
        let link_path = self.inode_map.get_path(id).ok_or(nfsstat3::NFS3ERR_NOENT)?;
        log::debug!("readlink-ing {}", link_path);
        let sftp = self.sftp().await?;
        let target_path = sftp.readlink(&link_path).await.map_err(Self::map_sftp_error)?;
        // TODO: do we need sftp to give us a string just to turn it back into vec<u8>?
        Ok(target_path.into_bytes().into())
    }
}
