//! SFTP Protocol Implementation
//!
//! A pure Rust implementation of SFTP protocol version 3, using SSH as a transport.
//! This implementation spawns an SSH subprocess and communicates via SFTP subsystem.

use bytes::{Buf, BufMut, BytesMut};
use chrono::{DateTime, Utc};
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;

// ============================================================================
// SFTP Protocol Constants
// ============================================================================

// SFTP Protocol Version 3 - Message Types
const SSH_FXP_INIT: u8 = 1;
const SSH_FXP_VERSION: u8 = 2;
const SSH_FXP_OPEN: u8 = 3;
const SSH_FXP_CLOSE: u8 = 4;
const SSH_FXP_READ: u8 = 5;
const SSH_FXP_WRITE: u8 = 6;
const SSH_FXP_LSTAT: u8 = 7;
const SSH_FXP_FSTAT: u8 = 8;
const SSH_FXP_SETSTAT: u8 = 9;
const SSH_FXP_FSETSTAT: u8 = 10;
const SSH_FXP_OPENDIR: u8 = 11;
const SSH_FXP_READDIR: u8 = 12;
const SSH_FXP_REMOVE: u8 = 13;
const SSH_FXP_MKDIR: u8 = 14;
const SSH_FXP_RMDIR: u8 = 15;
const SSH_FXP_REALPATH: u8 = 16;
const SSH_FXP_STAT: u8 = 17;
const SSH_FXP_RENAME: u8 = 18;
const SSH_FXP_READLINK: u8 = 19;
const SSH_FXP_SYMLINK: u8 = 20;

// Response types
const SSH_FXP_STATUS: u8 = 101;
const SSH_FXP_HANDLE: u8 = 102;
const SSH_FXP_DATA: u8 = 103;
const SSH_FXP_NAME: u8 = 104;
const SSH_FXP_ATTRS: u8 = 105;

// Status codes
const SSH_FX_OK: u32 = 0;
const SSH_FX_EOF: u32 = 1;
const SSH_FX_NO_SUCH_FILE: u32 = 2;
const SSH_FX_PERMISSION_DENIED: u32 = 3;
const SSH_FX_FAILURE: u32 = 4;
const SSH_FX_BAD_MESSAGE: u32 = 5;
const SSH_FX_NO_CONNECTION: u32 = 6;
const SSH_FX_CONNECTION_LOST: u32 = 7;
const SSH_FX_OP_UNSUPPORTED: u32 = 8;

// Attribute Flags
const SSH_FILEXFER_ATTR_SIZE: u32 = 0x00000001;
const SSH_FILEXFER_ATTR_UIDGID: u32 = 0x00000002;
const SSH_FILEXFER_ATTR_PERMISSIONS: u32 = 0x00000004;
const SSH_FILEXFER_ATTR_ACMODTIME: u32 = 0x00000008;
const SSH_FILEXFER_ATTR_EXTENDED: u32 = 0x80000000;

// Open Flags
const SSH_FXF_READ: u32 = 0x00000001;
const SSH_FXF_WRITE: u32 = 0x00000002;
const SSH_FXF_APPEND: u32 = 0x00000004;
const SSH_FXF_CREAT: u32 = 0x00000008;
const SSH_FXF_TRUNC: u32 = 0x00000010;
const SSH_FXF_EXCL: u32 = 0x00000020;

// ============================================================================
// Error Types
// ============================================================================

#[derive(Debug, Error)]
pub enum SFTPError {
    #[error("Not connected")]
    NotConnected,

    #[error("Connection lost")]
    ConnectionLost,

    #[error("Connection closed")]
    ConnectionClosed,

    #[error("Protocol error: {0}")]
    ProtocolError(String),

    #[error("Unexpected response")]
    UnexpectedResponse,

    #[error("Server error {0}: {1}")]
    ServerError(u32, String),

    #[error("Process spawn failed: {0}")]
    ProcessSpawnFailed(String),

    #[error("Read error: {0}")]
    ReadError(String),

    #[error("Write error: {0}")]
    WriteError(String),

    #[error("IO error: {0}")]
    IoError(#[from] io::Error),

    #[error("Response channel closed")]
    ResponseChannelClosed,
}

pub type Result<T> = std::result::Result<T, SFTPError>;

// ============================================================================
// SFTP Buffer - Binary Protocol Encoder/Decoder
// ============================================================================

/// Handles binary protocol encoding/decoding for SFTP wire format
struct SFTPBuffer {
    data: BytesMut,
}

impl SFTPBuffer {
    fn new() -> Self {
        Self {
            data: BytesMut::new(),
        }
    }

    fn from_bytes(data: Vec<u8>) -> Self {
        Self {
            data: BytesMut::from(&data[..]),
        }
    }

    // Writing (Building Packets)

    fn append_u8(&mut self, value: u8) {
        self.data.put_u8(value);
    }

    fn append_u32(&mut self, value: u32) {
        self.data.put_u32(value);
    }

    fn append_u64(&mut self, value: u64) {
        self.data.put_u64(value);
    }

    fn append_string(&mut self, value: &str) {
        let bytes = value.as_bytes();
        self.data.put_u32(bytes.len() as u32);
        self.data.put_slice(bytes);
    }

    fn append_data(&mut self, value: &[u8]) {
        self.data.put_u32(value.len() as u32);
        self.data.put_slice(value);
    }

    /// Append file attributes to the buffer
    ///
    /// Pass None for fields you don't want to set (server will use defaults)
    fn append_attrs(
        &mut self,
        size: Option<u64>,
        uid_gid: Option<(u32, u32)>,
        permissions: Option<u32>,
        atime_mtime: Option<(SystemTime, SystemTime)>,
    ) {
        // Calculate flags
        let mut flags = 0u32;

        if size.is_some() {
            flags |= SSH_FILEXFER_ATTR_SIZE;
        }
        if uid_gid.is_some() {
            flags |= SSH_FILEXFER_ATTR_UIDGID;
        }
        if permissions.is_some() {
            flags |= SSH_FILEXFER_ATTR_PERMISSIONS;
        }
        if atime_mtime.is_some() {
            flags |= SSH_FILEXFER_ATTR_ACMODTIME;
        }

        // Write flags first
        self.append_u32(flags);

        // Write optional fields in order
        if let Some(s) = size {
            self.append_u64(s);
        }
        if let Some((uid, gid)) = uid_gid {
            self.append_u32(uid);
            self.append_u32(gid);
        }
        if let Some(perms) = permissions {
            self.append_u32(perms);
        }
        if let Some((atime, mtime)) = atime_mtime {
            self.append_u32(atime.duration_since(UNIX_EPOCH).unwrap().as_secs() as u32);
            self.append_u32(mtime.duration_since(UNIX_EPOCH).unwrap().as_secs() as u32);
        }
    }

    // Reading (Parsing Responses)

    fn read_u8(&mut self) -> Option<u8> {
        if self.data.remaining() >= 1 {
            Some(self.data.get_u8())
        } else {
            None
        }
    }

    fn read_u32(&mut self) -> Option<u32> {
        if self.data.remaining() >= 4 {
            Some(self.data.get_u32())
        } else {
            None
        }
    }

    fn read_u64(&mut self) -> Option<u64> {
        if self.data.remaining() >= 8 {
            Some(self.data.get_u64())
        } else {
            None
        }
    }

    fn read_string(&mut self) -> Option<String> {
        let len = self.read_u32()? as usize;
        if self.data.remaining() >= len {
            let bytes = self.data.copy_to_bytes(len);
            String::from_utf8(bytes.to_vec()).ok()
        } else {
            None
        }
    }

    fn read_data(&mut self) -> Option<Vec<u8>> {
        let len = self.read_u32()? as usize;
        if self.data.remaining() >= len {
            Some(self.data.copy_to_bytes(len).to_vec())
        } else {
            None
        }
    }

    fn remaining(&self) -> Vec<u8> {
        self.data.to_vec()
    }

    fn to_vec(self) -> Vec<u8> {
        self.data.to_vec()
    }
}

// ============================================================================
// Public API Types
// ============================================================================

/// Status RequestID with pending response channel
pub struct SFTPStatusRequestID {
    request_id: u32,
    receiver: oneshot::Receiver<Result<SFTPResponse>>,
}

/// File Attributes RequestID with pending response channel
pub struct SFTPFileAttrsRequestID {
    request_id: u32,
    receiver: oneshot::Receiver<Result<SFTPResponse>>,
}

/// File or directory attributes from SFTP
#[derive(Debug, Clone)]
pub struct FileAttributes {
    pub size: u64,
    pub uid: u32,
    pub gid: u32,
    pub permissions: u32,
    pub access_time: SystemTime,
    pub modification_time: SystemTime,
}

impl FileAttributes {
    /// Check if this is a directory
    pub fn is_directory(&self) -> bool {
        // S_IFDIR = 0x4000
        (self.permissions & 0x4000) != 0
    }

    fn format_permissions(perms: u32) -> String {
        // File type (S_IFMT mask = 0xF000)
        let file_type = perms & 0xF000;
        let type_char = match file_type {
            0x4000 => 'd', // S_IFDIR
            0xA000 => 'l', // S_IFLNK
            0x8000 => '-', // S_IFREG
            0x6000 => 'b', // S_IFBLK
            0x2000 => 'c', // S_IFCHR
            0x1000 => 'p', // S_IFIFO
            0xC000 => 's', // S_IFSOCK
            _ => '?',
        };

        // Owner permissions
        let ur = if perms & 0o400 != 0 { 'r' } else { '-' };
        let uw = if perms & 0o200 != 0 { 'w' } else { '-' };
        let ux = if perms & 0o100 != 0 { 'x' } else { '-' };

        // Group permissions
        let gr = if perms & 0o040 != 0 { 'r' } else { '-' };
        let gw = if perms & 0o020 != 0 { 'w' } else { '-' };
        let gx = if perms & 0o010 != 0 { 'x' } else { '-' };

        // Other permissions
        let or = if perms & 0o004 != 0 { 'r' } else { '-' };
        let ow = if perms & 0o002 != 0 { 'w' } else { '-' };
        let ox = if perms & 0o001 != 0 { 'x' } else { '-' };

        format!(
            "{}{}{}{}{}{}{}{}{}{}",
            type_char, ur, uw, ux, gr, gw, gx, or, ow, ox
        )
    }
}

impl fmt::Display for FileAttributes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let perms = Self::format_permissions(self.permissions);
        let mtime: DateTime<Utc> = self.modification_time.into();

        write!(
            f,
            "{} {:8} {:8} {:8} {}",
            perms,
            self.uid,
            self.gid,
            self.size,
            mtime.format("%b %d %H:%M")
        )
    }
}

/// Opaque handle to an open file or directory on the SFTP server
#[derive(Debug, Clone)]
pub struct SFTPHandle {
    data: Vec<u8>,
}

impl SFTPHandle {
    fn new(data: Vec<u8>) -> Self {
        Self { data }
    }
}

bitflags::bitflags! {
    /// Flags for opening files
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct SFTPOpenFlags: u32 {
        const READ = SSH_FXF_READ;
        const WRITE = SSH_FXF_WRITE;
        const APPEND = SSH_FXF_APPEND;
        const CREATE = SSH_FXF_CREAT;
        const TRUNCATE = SSH_FXF_TRUNC;
        const EXCLUSIVE = SSH_FXF_EXCL;
    }
}

/// Directory entry returned by list_directory
#[derive(Debug, Clone)]
pub struct DirectoryEntry {
    pub filename: String,
    pub attributes: FileAttributes,
}

/// Internal type representing a parsed SFTP response packet
#[derive(Debug)]
struct SFTPResponse {
    type_: u8,
    request_id: u32,
    payload: Vec<u8>,
}

impl fmt::Display for SFTPResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SFTPResponse: id: {} type: {}",
            self.request_id, self.type_
        )
    }
}

// ============================================================================
// Thread-Safe Helper Types
// ============================================================================

/// Thread-safe counter for generating unique request IDs
struct RequestIdCounter {
    value: AtomicU32,
}

impl RequestIdCounter {
    fn new() -> Self {
        Self {
            value: AtomicU32::new(1),
        }
    }

    fn next(&self) -> u32 {
        self.value.fetch_add(1, Ordering::SeqCst)
    }
}

/// Type for pending request continuations
type PendingRequests = Arc<Mutex<HashMap<u32, oneshot::Sender<Result<SFTPResponse>>>>>;

/// SSH process state
struct ProcessState {
    child: Child,
    stdin: ChildStdin,
}

// ============================================================================
// Main SFTP Connection
// ============================================================================

/// Main SFTP connection class
///
/// Simple design: no actors, just locks and channels
pub struct SFTPConnection {
    // Connection configuration (immutable)
    hostname: String,
    port: u16,
    username: String,

    // SSH process and pipes (protected by mutex)
    process: Arc<Mutex<Option<ProcessState>>>,

    // Thread-safe shared state
    pending_requests: PendingRequests,
    request_id_counter: RequestIdCounter,

    // Background reader task
    reader_task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl SFTPConnection {
    /// Create a new SFTP connection
    pub fn new(hostname: String, port: u16, username: String) -> Self {
        Self {
            hostname,
            port,
            username,
            process: Arc::new(Mutex::new(None)),
            pending_requests: Arc::new(Mutex::new(HashMap::new())),
            request_id_counter: RequestIdCounter::new(),
            reader_task: Arc::new(Mutex::new(None)),
        }
    }

    /// Connect to the SFTP server
    pub async fn connect(&self) -> Result<()> {
        info!("Connecting to {}@{}:{}", self.username, self.hostname, self.port);

        // Create SSH process
        let mut child = Command::new("ssh")
            .arg("-p")
            .arg(self.port.to_string())
            .arg("-o")
            .arg("ServerAliveInterval=15")
            .arg("-o")
            .arg("ServerAliveCountMax=3")
            .arg("-s")
            .arg(format!("{}@{}", self.username, self.hostname))
            .arg("sftp")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| SFTPError::ProcessSpawnFailed(e.to_string()))?;

        debug!("SSH process started (PID: {:?})", child.id());

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| SFTPError::ProcessSpawnFailed("Failed to get stdin".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| SFTPError::ProcessSpawnFailed("Failed to get stdout".into()))?;

        // Store process state (without stdout - it's moved to reader task)
        let process_state = ProcessState {
            child,
            stdin,
        };
        *self.process.lock().await = Some(process_state);

        // Start background reader task with stdout
        self.start_reader_task(stdout);

        // Send INIT and wait for VERSION
        self.send_init().await?;
        info!("SFTP connection established");

        Ok(())
    }

    /// Disconnect from the SFTP server
    pub async fn disconnect(&self) {
        info!("Disconnecting");

        // Cancel reader task
        if let Some(task) = self.reader_task.lock().await.take() {
            task.abort();
        }

        // Terminate SSH process
        if let Some(mut state) = self.process.lock().await.take() {
            let _ = state.child.kill().await;
        }

        // Fail all pending requests
        let pending = {
            let mut map = self.pending_requests.lock().await;
            std::mem::take(&mut *map)
        };

        for (_, tx) in pending {
            let _ = tx.send(Err(SFTPError::ConnectionClosed));
        }

        debug!("Disconnected");
    }

    // ========================================================================
    // Public Operations
    // ========================================================================

    /// List the contents of a directory
    pub async fn list_directory(&self, path: &str) -> Result<Vec<DirectoryEntry>> {
        debug!("Listing directory: {}", path);

        // Open the directory
        let handle = self.open_directory(path).await?;

        // Read all entries
        let mut all_entries = Vec::new();
        loop {
            let entries = self.read_directory(&handle).await?;
            if entries.is_empty() {
                break; // EOF
            }
            all_entries.extend(entries);
        }

        // Close the directory
        self.close(handle).await?;

        debug!("Listed {} entries in {}", all_entries.len(), path);
        Ok(all_entries)
    }

    /// Get file attributes (follows symlinks)
    pub async fn stat(&self, path: &str) -> Result<FileAttributes> {
        self.stat_generic(path, SSH_FXP_STAT).await
    }

    /// Get file attributes (does not follow symlinks)
    pub async fn lstat(&self, path: &str) -> Result<FileAttributes> {
        self.stat_generic(path, SSH_FXP_LSTAT).await
    }

    /// Read the target of a symbolic link
    pub async fn readlink(&self, path: &str) -> Result<String> {
        debug!("Reading symlink: {}", path);
        let request_id = self.request_id_counter.next();

        // Build READLINK packet
        let mut buffer = SFTPBuffer::new();
        buffer.append_u8(SSH_FXP_READLINK);
        buffer.append_u32(request_id);
        buffer.append_string(path);

        self.send_packet(buffer).await?;

        // Wait for response
        let response = self.wait_for_response(request_id).await?;

        debug!("readlink: got response: {}", response);

        // Should get NAME response with one entry
        if response.type_ == SSH_FXP_NAME {
            let mut buffer = SFTPBuffer::from_bytes(response.payload);

            let count = buffer
                .read_u32()
                .ok_or_else(|| SFTPError::ProtocolError("Failed to parse name count".into()))?;

            if count != 1 {
                return Err(SFTPError::ProtocolError(format!(
                    "Expected 1 name in readlink response, got {}",
                    count
                )));
            }

            // Read the target path (filename field contains the target)
            let target = buffer
                .read_string()
                .ok_or_else(|| SFTPError::ProtocolError("Failed to parse link target".into()))?;

            // Skip longname (dummy field)
            let _ = buffer.read_string();

            // Skip attributes (dummy field)
            // We don't need to fully parse them, just skip past them

            debug!("Symlink {} -> {}", path, target);
            Ok(target)
        } else if response.type_ == SSH_FXP_STATUS {
            let (code, message) = Self::parse_status(&response.payload)?;
            Err(SFTPError::ServerError(code, message))
        } else {
            Err(SFTPError::UnexpectedResponse)
        }
    }

    /// Get file attributes for an open file
    pub async fn fstat(&self, handle: &SFTPHandle) -> Result<FileAttributes> {
        let request_id = self.request_id_counter.next();

        // Build FSTAT packet
        let mut buffer = SFTPBuffer::new();
        buffer.append_u8(SSH_FXP_FSTAT);
        buffer.append_u32(request_id);
        buffer.append_data(&handle.data);

        self.send_packet(buffer).await?;

        // Wait for response
        let response = self.wait_for_response(request_id).await?;

        debug!("fstat: got response: {}", response);

        // Should get ATTRS response
        if response.type_ == SSH_FXP_ATTRS {
            let mut buffer = SFTPBuffer::from_bytes(response.payload);
            Self::parse_attributes(&mut buffer)
        } else if response.type_ == SSH_FXP_STATUS {
            let (code, message) = Self::parse_status(&response.payload)?;
            Err(SFTPError::ServerError(code, message))
        } else {
            Err(SFTPError::UnexpectedResponse)
        }
    }

    /// Get file attributes for an open file without waiting
    pub async fn fstat_nowait(&self, handle: &SFTPHandle) -> Result<SFTPFileAttrsRequestID> {
        let request_id = self.request_id_counter.next();

        // Create channel and register BEFORE sending (avoids race condition)
        let (tx, rx) = oneshot::channel();
        self.pending_requests.lock().await.insert(request_id, tx);

        // Build FSTAT packet
        let mut buffer = SFTPBuffer::new();
        buffer.append_u8(SSH_FXP_FSTAT);
        buffer.append_u32(request_id);
        buffer.append_data(&handle.data);

        self.send_packet(buffer).await?;

        Ok(SFTPFileAttrsRequestID {
            request_id,
            receiver: rx,
        })
    }

    /// Wait for the response to a request which returns FileAttributes
    pub async fn wait_for_fileattrs_response(&self, req: SFTPFileAttrsRequestID) -> Result<FileAttributes> {
        // Wait for response from the pre-registered channel
        let response = req.receiver.await
            .map_err(|_| SFTPError::ResponseChannelClosed)??;

        debug!("fstat_nowait: got response: {}", response);

        // Must get ATTRS response
        if response.type_ == SSH_FXP_ATTRS {
            let mut buffer = SFTPBuffer::from_bytes(response.payload);
            Self::parse_attributes(&mut buffer)
        } else if response.type_ == SSH_FXP_STATUS {
            let (code, message) = Self::parse_status(&response.payload)?;
            Err(SFTPError::ServerError(code, message))
        } else {
            Err(SFTPError::UnexpectedResponse)
        }
    }

    /// Open a file
    pub async fn open(&self, path: &str, flags: SFTPOpenFlags) -> Result<SFTPHandle> {
        let request_id = self.request_id_counter.next();

        // Build OPEN packet
        let mut buffer = SFTPBuffer::new();
        buffer.append_u8(SSH_FXP_OPEN);
        buffer.append_u32(request_id);
        buffer.append_string(path);
        buffer.append_u32(flags.bits());
        buffer.append_u32(0); // ATTRS field - empty

        self.send_packet(buffer).await?;

        // Wait for response
        let response = self.wait_for_response(request_id).await?;

        debug!("open: got response: {}", response);

        // Should get HANDLE response
        if response.type_ == SSH_FXP_HANDLE {
            let mut payload = SFTPBuffer::from_bytes(response.payload);
            let handle_data = payload
                .read_data()
                .ok_or_else(|| SFTPError::ProtocolError("Failed to parse file handle".into()))?;
            Ok(SFTPHandle::new(handle_data))
        } else if response.type_ == SSH_FXP_STATUS {
            let (code, message) = Self::parse_status(&response.payload)?;
            Err(SFTPError::ServerError(code, message))
        } else {
            Err(SFTPError::UnexpectedResponse)
        }
    }

    /// Create a file, not exclusive
    pub async fn create(&self, path: &str, attrs: &FileAttributes) -> Result<SFTPHandle> {
        let request_id = self.request_id_counter.next();
        // TODO: check that these are the right flags
        let flags = SFTPOpenFlags::READ | SFTPOpenFlags::CREATE;

        // Build OPEN packet
        let mut buffer = SFTPBuffer::new();
        buffer.append_u8(SSH_FXP_OPEN);
        buffer.append_u32(request_id);
        buffer.append_string(path);
        buffer.append_u32(flags.bits());
        buffer.append_attrs(
            Some(attrs.size),
            Some((attrs.uid, attrs.gid)),
            Some(attrs.permissions), 
            Some((attrs.access_time, attrs.modification_time))
        );

        self.send_packet(buffer).await?;

        // Wait for response
        let response = self.wait_for_response(request_id).await?;

        debug!("create: got response: {}", response);

        // Should get HANDLE response
        if response.type_ == SSH_FXP_HANDLE {
            let mut payload = SFTPBuffer::from_bytes(response.payload);
            let handle_data = payload
                .read_data()
                .ok_or_else(|| SFTPError::ProtocolError("Failed to parse file handle".into()))?;
            Ok(SFTPHandle::new(handle_data))
        } else if response.type_ == SSH_FXP_STATUS {
            let (code, message) = Self::parse_status(&response.payload)?;
            Err(SFTPError::ServerError(code, message))
        } else {
            Err(SFTPError::UnexpectedResponse)
        }
    }
    ///
    /// Delete a file
    pub async fn remove(&self, path: &str) -> Result<()> {
        let request_id = self.request_id_counter.next();

        // Build REMOVE packet
        let mut buffer = SFTPBuffer::new();
        buffer.append_u8(SSH_FXP_REMOVE);
        buffer.append_u32(request_id);
        buffer.append_string(path);

        self.send_packet(buffer).await?;

        // Wait for response
        let response = self.wait_for_response(request_id).await?;

        debug!("remove: got response: {}", response);

        // Should get STATUS response
        if response.type_ == SSH_FXP_STATUS {
            let (code, message) = Self::parse_status(&response.payload)?;
            if code == SSH_FX_OK {
                Ok(())
            } else {
                Err(SFTPError::ServerError(code, message))
            }
        } else {
            error!("delete: got unexpected response: {}", response);
            Err(SFTPError::UnexpectedResponse)
        }
    }

    /// Read from a file
    pub async fn read(&self, handle: &SFTPHandle, offset: u64, length: u32) -> Result<Vec<u8>> {
        let request_id = self.request_id_counter.next();

        // Build READ packet
        let mut buffer = SFTPBuffer::new();
        buffer.append_u8(SSH_FXP_READ);
        buffer.append_u32(request_id);
        buffer.append_data(&handle.data);
        buffer.append_u64(offset);
        buffer.append_u32(length);

        self.send_packet(buffer).await?;

        // Wait for response
        let response = self.wait_for_response(request_id).await?;

        debug!("read: got response: {}", response);

        // Check for STATUS (might be EOF)
        if response.type_ == SSH_FXP_STATUS {
            let (code, message) = Self::parse_status(&response.payload)?;
            if code == SSH_FX_EOF {
                return Ok(Vec::new()); // EOF
            }
            return Err(SFTPError::ServerError(code, message));
        }

        // Should get DATA response
        if response.type_ == SSH_FXP_DATA {
            let mut payload = SFTPBuffer::from_bytes(response.payload);
            let data = payload
                .read_data()
                .ok_or_else(|| SFTPError::ProtocolError("Failed to parse read data".into()))?;
            Ok(data)
        } else {
            Err(SFTPError::UnexpectedResponse)
        }
    }

    /// Write to a file
    pub async fn write(&self, handle: &SFTPHandle, offset: u64, data: &[u8]) -> Result<()> {
        let request_id = self.request_id_counter.next();

        // Build WRITE packet
        let mut buffer = SFTPBuffer::new();
        buffer.append_u8(SSH_FXP_WRITE);
        buffer.append_u32(request_id);
        buffer.append_data(&handle.data);
        buffer.append_u64(offset);
        buffer.append_data(data);

        self.send_packet(buffer).await?;

        // Wait for response
        let response = self.wait_for_response(request_id).await?;

        debug!("write: got response: {}", response);

        // Should get STATUS response
        if response.type_ == SSH_FXP_STATUS {
            let (code, message) = Self::parse_status(&response.payload)?;
            if code == SSH_FX_OK {
                Ok(())
            } else {
                Err(SFTPError::ServerError(code, message))
            }
        } else {
            error!("write: got unexpected response: {}", response);
            Err(SFTPError::UnexpectedResponse)
        }
    }

    /// Write to a file without waiting for response
    pub async fn write_nowait(&self, handle: &SFTPHandle, offset: u64, data: &[u8]) -> Result<SFTPStatusRequestID> {
        let request_id = self.request_id_counter.next();

        // Create channel and register BEFORE sending (avoids race condition)
        let (tx, rx) = oneshot::channel();
        self.pending_requests.lock().await.insert(request_id, tx);

        // Build WRITE packet
        let mut buffer = SFTPBuffer::new();
        buffer.append_u8(SSH_FXP_WRITE);
        buffer.append_u32(request_id);
        buffer.append_data(&handle.data);
        buffer.append_u64(offset);
        buffer.append_data(data);

        self.send_packet(buffer).await?;

        Ok(SFTPStatusRequestID {
            request_id,
            receiver: rx,
        })
    }

    pub async fn wait_for_status_response(&self, req: SFTPStatusRequestID) -> Result<()> {
        // Wait for response from the pre-registered channel
        let response = req.receiver.await
            .map_err(|_| SFTPError::ResponseChannelClosed)??;

        debug!("write_nowait: got response: {}", response);

        // Must get STATUS response
        if response.type_ == SSH_FXP_STATUS {
            let (code, message) = Self::parse_status(&response.payload)?;
            if code == SSH_FX_OK {
                Ok(())
            } else {
                Err(SFTPError::ServerError(code, message))
            }
        } else {
            error!("write_nowait: got unexpected response: {}", response);
            Err(SFTPError::UnexpectedResponse)
        }
    }

    /// Close a file or directory handle
    pub async fn close(&self, handle: SFTPHandle) -> Result<()> {
        let request_id = self.request_id_counter.next();

        // Build CLOSE packet
        let mut buffer = SFTPBuffer::new();
        buffer.append_u8(SSH_FXP_CLOSE);
        buffer.append_u32(request_id);
        buffer.append_data(&handle.data);

        self.send_packet(buffer).await?;

        // Wait for response
        let response = self.wait_for_response(request_id).await?;

        debug!("close: got response: {}", response);

        // Should get STATUS response
        if response.type_ == SSH_FXP_STATUS {
            let (code, message) = Self::parse_status(&response.payload)?;
            if code == SSH_FX_OK {
                Ok(())
            } else {
                Err(SFTPError::ServerError(code, message))
            }
        } else {
            error!("close: got unexpected response: {}", response);
            Err(SFTPError::UnexpectedResponse)
        }
    }

    // ========================================================================
    // Private Helpers
    // ========================================================================

    fn start_reader_task(&self, stdout: ChildStdout) {
        let pending_requests = Arc::clone(&self.pending_requests);

        let task = tokio::spawn(async move {
            Self::read_responses_loop(stdout, pending_requests).await;
        });

        // Store the task handle (note: can't use blocking lock in async context)
        let reader_task = Arc::clone(&self.reader_task);
        tokio::spawn(async move {
            *reader_task.lock().await = Some(task);
        });
    }

    async fn read_responses_loop(
        mut stdout: ChildStdout,
        pending_requests: PendingRequests,
    ) {
        debug!("Reader task started");

        // Read responses until cancelled or error
        loop {
            // Read 4-byte length header
            let mut length_buf = [0u8; 4];
            match stdout.read_exact(&mut length_buf).await {
                Ok(_) => {}
                Err(e) => {
                    warn!("Failed to read packet length: {}", e);
                    break;
                }
            }

            let length = u32::from_be_bytes(length_buf);
            debug!("Reading packet of length {}", length);

            // Read packet payload
            let mut payload = vec![0u8; length as usize];
            match stdout.read_exact(&mut payload).await {
                Ok(_) => {}
                Err(e) => {
                    warn!("Failed to read packet payload: {}", e);
                    break;
                }
            }

            // Parse the response
            let mut buffer = SFTPBuffer::from_bytes(payload);
            let type_ = match buffer.read_u8() {
                Some(t) => t,
                None => {
                    error!("Failed to parse packet type");
                    continue;
                }
            };

            debug!("Received packet type {}", type_);

            // Determine request ID
            let request_id = if type_ == SSH_FXP_VERSION {
                0 // VERSION doesn't have a request ID
            } else {
                match buffer.read_u32() {
                    Some(id) => id,
                    None => {
                        error!("Failed to parse request ID");
                        continue;
                    }
                }
            };

            debug!("Response for request ID {}", request_id);

            // Create response object
            let response = SFTPResponse {
                type_,
                request_id,
                payload: buffer.remaining(),
            };

            // Wake up the waiting request
            let tx = pending_requests.lock().await.remove(&request_id);
            if let Some(tx) = tx {
                debug!("Resuming continuation for request {}", request_id);
                let _ = tx.send(Ok(response));
            } else {
                warn!("No pending request for ID {}", request_id);
            }
        }

        debug!("Reader task ended");

        // Fail all remaining pending requests
        let pending = {
            let mut map = pending_requests.lock().await;
            std::mem::take(&mut *map)
        };

        for (_, tx) in pending {
            let _ = tx.send(Err(SFTPError::ConnectionLost));
        }
    }

    async fn send_init(&self) -> Result<()> {
        debug!("Sending INIT");

        // Build INIT packet
        let mut buffer = SFTPBuffer::new();
        buffer.append_u8(SSH_FXP_INIT);
        buffer.append_u32(3); // SFTP protocol version 3

        self.send_packet(buffer).await?;

        // Wait for VERSION response (uses request ID 0)
        let response = self.wait_for_response(0).await?;

        // Verify it's a VERSION response
        if response.type_ != SSH_FXP_VERSION {
            return Err(SFTPError::ProtocolError(format!(
                "Expected VERSION, got {}",
                response.type_
            )));
        }

        // Parse the version number
        let mut payload = SFTPBuffer::from_bytes(response.payload);
        let version = payload
            .read_u32()
            .ok_or_else(|| SFTPError::ProtocolError("Failed to parse VERSION".into()))?;

        info!("SFTP server version: {}", version);
        Ok(())
    }

    async fn send_packet(&self, buffer: SFTPBuffer) -> Result<()> {
        let payload = buffer.to_vec();
        let length = payload.len() as u32;

        // Build packet: [4-byte length][payload]
        let mut packet = Vec::with_capacity(4 + payload.len());
        packet.extend_from_slice(&length.to_be_bytes());
        packet.extend_from_slice(&payload);

        // Write to stdin
        let mut guard = self.process.lock().await;
        let state = guard
            .as_mut()
            .ok_or(SFTPError::NotConnected)?;

        state
            .stdin
            .write_all(&packet)
            .await
            .map_err(|e| SFTPError::WriteError(e.to_string()))?;

        Ok(())
    }

    async fn wait_for_response(&self, request_id: u32) -> Result<SFTPResponse> {
        let (tx, rx) = oneshot::channel();

        // Store the sender
        self.pending_requests.lock().await.insert(request_id, tx);

        // Wait for response
        rx.await
            .map_err(|_| SFTPError::ResponseChannelClosed)?
    }

    // ========================================================================
    // SFTP Protocol Operations
    // ========================================================================

    async fn open_directory(&self, path: &str) -> Result<SFTPHandle> {
        let request_id = self.request_id_counter.next();

        // Build OPENDIR packet
        let mut buffer = SFTPBuffer::new();
        buffer.append_u8(SSH_FXP_OPENDIR);
        buffer.append_u32(request_id);
        buffer.append_string(path);

        self.send_packet(buffer).await?;

        // Wait for response
        let response = self.wait_for_response(request_id).await?;

        // Should get HANDLE response
        if response.type_ == SSH_FXP_HANDLE {
            let mut payload = SFTPBuffer::from_bytes(response.payload);
            let handle_data = payload
                .read_data()
                .ok_or_else(|| SFTPError::ProtocolError("Failed to parse directory handle".into()))?;
            Ok(SFTPHandle::new(handle_data))
        } else if response.type_ == SSH_FXP_STATUS {
            let (code, message) = Self::parse_status(&response.payload)?;
            Err(SFTPError::ServerError(code, message))
        } else {
            Err(SFTPError::UnexpectedResponse)
        }
    }

    async fn stat_generic(&self, path: &str, stat_type: u8) -> Result<FileAttributes> {
        assert!(stat_type == SSH_FXP_STAT || stat_type == SSH_FXP_LSTAT);

        let request_id = self.request_id_counter.next();

        // Build STAT/LSTAT packet
        let mut buffer = SFTPBuffer::new();
        buffer.append_u8(stat_type);
        buffer.append_u32(request_id);
        buffer.append_string(path);

        self.send_packet(buffer).await?;

        // Wait for response
        let response = self.wait_for_response(request_id).await?;

        let op_name = if stat_type == SSH_FXP_STAT {
            "stat"
        } else {
            "lstat"
        };
        debug!("{}: got response: {}", op_name, response);

        // Should get ATTRS response
        if response.type_ == SSH_FXP_ATTRS {
            let mut buffer = SFTPBuffer::from_bytes(response.payload);
            Self::parse_attributes(&mut buffer)
        } else if response.type_ == SSH_FXP_STATUS {
            let (code, message) = Self::parse_status(&response.payload)?;
            Err(SFTPError::ServerError(code, message))
        } else {
            Err(SFTPError::UnexpectedResponse)
        }
    }

    async fn read_directory(&self, handle: &SFTPHandle) -> Result<Vec<DirectoryEntry>> {
        let request_id = self.request_id_counter.next();

        // Build READDIR packet
        let mut buffer = SFTPBuffer::new();
        buffer.append_u8(SSH_FXP_READDIR);
        buffer.append_u32(request_id);
        buffer.append_data(&handle.data);

        self.send_packet(buffer).await?;

        // Wait for response
        let response = self.wait_for_response(request_id).await?;

        // Check for STATUS (might be EOF)
        if response.type_ == SSH_FXP_STATUS {
            let (code, message) = Self::parse_status(&response.payload)?;
            if code == SSH_FX_EOF {
                return Ok(Vec::new()); // No more entries
            }
            return Err(SFTPError::ServerError(code, message));
        }

        // Should get NAME response
        if response.type_ == SSH_FXP_NAME {
            Self::parse_directory_entries(&response.payload)
        } else {
            Err(SFTPError::UnexpectedResponse)
        }
    }

    // ========================================================================
    // Parsing Helpers
    // ========================================================================

    fn parse_status(data: &[u8]) -> Result<(u32, String)> {
        let mut buffer = SFTPBuffer::from_bytes(data.to_vec());
        let code = buffer
            .read_u32()
            .ok_or_else(|| SFTPError::ProtocolError("Failed to parse status code".into()))?;
        let message = buffer.read_string().unwrap_or_default();
        Ok((code, message))
    }

    fn parse_directory_entries(data: &[u8]) -> Result<Vec<DirectoryEntry>> {
        let mut buffer = SFTPBuffer::from_bytes(data.to_vec());

        let count = buffer
            .read_u32()
            .ok_or_else(|| SFTPError::ProtocolError("Failed to parse entry count".into()))?;

        let mut entries = Vec::new();
        for _ in 0..count {
            let filename = buffer
                .read_string()
                .ok_or_else(|| SFTPError::ProtocolError("Failed to parse filename".into()))?;

            // Read longname (we don't need it, but have to skip it)
            let _ = buffer
                .read_string()
                .ok_or_else(|| SFTPError::ProtocolError("Failed to parse longname".into()))?;

            let attributes = Self::parse_attributes(&mut buffer)?;

            entries.push(DirectoryEntry {
                filename,
                attributes,
            });
        }

        Ok(entries)
    }

    fn parse_attributes(buffer: &mut SFTPBuffer) -> Result<FileAttributes> {
        let flags = buffer
            .read_u32()
            .ok_or_else(|| SFTPError::ProtocolError("Failed to parse attribute flags".into()))?;

        let has_size = (flags & SSH_FILEXFER_ATTR_SIZE) != 0;
        let has_uidgid = (flags & SSH_FILEXFER_ATTR_UIDGID) != 0;
        let has_permissions = (flags & SSH_FILEXFER_ATTR_PERMISSIONS) != 0;
        let has_acmodtime = (flags & SSH_FILEXFER_ATTR_ACMODTIME) != 0;

        let size = if has_size {
            buffer
                .read_u64()
                .ok_or_else(|| SFTPError::ProtocolError("Failed to parse size".into()))?
        } else {
            0
        };

        let (uid, gid) = if has_uidgid {
            let u = buffer
                .read_u32()
                .ok_or_else(|| SFTPError::ProtocolError("Failed to parse uid".into()))?;
            let g = buffer
                .read_u32()
                .ok_or_else(|| SFTPError::ProtocolError("Failed to parse gid".into()))?;
            (u, g)
        } else {
            (0, 0)
        };

        let permissions = if has_permissions {
            buffer
                .read_u32()
                .ok_or_else(|| SFTPError::ProtocolError("Failed to parse permissions".into()))?
        } else {
            0
        };

        let (access_time, modification_time) = if has_acmodtime {
            let a = buffer
                .read_u32()
                .ok_or_else(|| SFTPError::ProtocolError("Failed to parse atime".into()))?;
            let m = buffer
                .read_u32()
                .ok_or_else(|| SFTPError::ProtocolError("Failed to parse mtime".into()))?;
            (
                UNIX_EPOCH + std::time::Duration::from_secs(a as u64),
                UNIX_EPOCH + std::time::Duration::from_secs(m as u64),
            )
        } else {
            (SystemTime::now(), SystemTime::now())
        };

        Ok(FileAttributes {
            size,
            uid,
            gid,
            permissions,
            access_time,
            modification_time,
        })
    }
}

mod tests {
    use std::time::SystemTime;
    use crate::sftp::FileAttributes;
    use crate::sftp::SFTPOpenFlags;
    use crate::sftp::SFTPConnection;

    #[tokio::test]
    async fn test_dir_listing_basic() {
        let conn = SFTPConnection::new("pop-os".into(), 22, "josh".into());
        assert!(conn.connect().await.is_ok());

        match conn.list_directory("/home/josh".into()).await {
            Ok(entries) => {
                for entry in entries {
                    // Skip . and ..
                    if entry.filename == "." || entry.filename == ".." {
                        continue;
                    }

                    let type_indicator = if entry.attributes.is_directory() {
                        "/"
                    } else {
                        ""
                    };

                    println!(
                        "{} {}{}",
                        entry.attributes, entry.filename, type_indicator
                    );
                }
            }
            Err(e) => {
                eprintln!("Error listing directory: {}", e);
                assert!(false);
            }
        }
        println!("Disconnecting...");
        conn.disconnect().await;
    }

    #[tokio::test]
    async fn test_read_basic() {
        let conn = SFTPConnection::new("pop-os".into(), 22, "josh".into());
        assert!(conn.connect().await.is_ok());

        let handle = conn.open("/home/josh/hello.txt", SFTPOpenFlags::READ).await.unwrap();

        let contents = conn.read(&handle, 0, 1024).await.unwrap();
        assert!(!contents.is_empty());
        assert!(contents == b"TEST\n");

        println!("Disconnecting...");
        assert!(conn.close(handle).await.is_ok());
        conn.disconnect().await;
    }

    #[tokio::test]
    async fn test_fstat_basic() {
        let conn = SFTPConnection::new("pop-os".into(), 22, "josh".into());
        assert!(conn.connect().await.is_ok());

        let handle = conn.open("/home/josh/hello.txt", SFTPOpenFlags::READ).await.unwrap();

        let stat = conn.fstat(&handle).await.unwrap();
        assert_eq!(stat.size, 5);

        println!("Disconnecting...");
        assert!(conn.close(handle).await.is_ok());
        conn.disconnect().await;
    }

    #[tokio::test]
    async fn test_readlink() {
        let conn = SFTPConnection::new("pop-os".into(), 22, "josh".into());
        assert!(conn.connect().await.is_ok());

        let target = conn.readlink("/home/josh/testlink").await.unwrap();
        assert_eq!("hello.txt", target);

        println!("Disconnecting...");
        conn.disconnect().await;
    }

    #[tokio::test]
    async fn test_create_and_remove() {
        let conn = SFTPConnection::new("pop-os".into(), 22, "josh".into());
        assert!(conn.connect().await.is_ok());

        let attrs = FileAttributes {
           size: 0,
           uid: 1000,  // Typical user uid
           gid: 1000,  // Typical user gid
           permissions: 0o644,  // rw-r--r--
           access_time: SystemTime::now(),
           modification_time: SystemTime::now(),
       };

        let handle = conn.create("/home/josh/testcreate", &attrs).await.unwrap();
        assert!(conn.close(handle).await.is_ok());

        assert!(conn.remove("/home/josh/testcreate").await.is_ok());

        println!("Disconnecting...");
        conn.disconnect().await;
    }
}
