use crate::inode_map::InodeMap;
use crate::sftp::SFTPConnection;

use async_trait::async_trait;
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

    /// Map SFTP errors to NFS errors
    fn map_sftp_error(e: crate::sftp::SFTPError) -> nfsstat3 {
        // TODO: More sophisticated error mapping
        log::error!("SFTP error: {}", e);
        nfsstat3::NFS3ERR_IO
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
        println!("lookup: dirid: {dirid}, filename: {:?}", filename);
        todo!();
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

        // Get SFTP file attributes
        let sftp = self.sftp().await?;
        let attrs = sftp.stat(&path).await.map_err(Self::map_sftp_error)?;

        // TODO: Convert FileAttributes -> fattr3
        log::debug!("getattr: got attrs {attrs} for {}: size={}", path, attrs.size);
        todo!("Convert FileAttributes to fattr3");
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
        todo!();
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
        println!("readdir id {dirid} with start after {start_after} and max {max_entries}");
        todo!();
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
        todo!()
    }
}
