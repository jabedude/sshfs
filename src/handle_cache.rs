//! File Handle Cache for SFTP
//!
//! Caches open SFTP file handles to avoid repeated OPEN/CLOSE operations.
//! Handles are automatically closed after being idle for a configurable timeout.
//! Also caches file attributes to reduce FSTAT round trips.

use crate::sftp::{FileAttributes, SFTPConnection, SFTPHandle, SFTPOpenFlags};
use log::{debug, info};
use nfsserve::nfs::{fileid3, nfsstat3};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// A cached file handle with metadata
struct CachedHandle {
    handle: SFTPHandle,
    last_used: Instant,
    path: String, // For debugging/validation

    // Attribute caching
    cached_attrs: Option<FileAttributes>,
    last_fstat: Option<Instant>,
}

/// Cache for SFTP file handles
///
/// Keeps file handles open between operations to reduce SSH round trips.
/// Automatically closes handles that have been idle for longer than the timeout.
pub struct HandleCache {
    /// Cached handles: file_id -> handle info
    handles: Arc<Mutex<HashMap<fileid3, CachedHandle>>>,

    /// How long before closing idle handles
    idle_timeout: Duration,

    /// How long to cache file attributes (reduces FSTAT calls)
    attr_cache_ttl: Duration,
}

impl HandleCache {
    /// Create a new handle cache
    ///
    /// # Arguments
    /// * `idle_timeout` - Duration after which unused handles are closed
    /// * `attr_cache_ttl` - Duration to cache file attributes (e.g., 100ms)
    pub fn new(idle_timeout: Duration, attr_cache_ttl: Duration) -> Self {
        Self {
            handles: Arc::new(Mutex::new(HashMap::new())),
            idle_timeout,
            attr_cache_ttl,
        }
    }

    /// Get a cached handle or open a new one
    ///
    /// # Arguments
    /// * `file_id` - NFS file ID
    /// * `path` - File path (used if we need to open)
    /// * `sftp` - SFTP connection to use for opening
    ///
    /// # Returns
    /// Cloned handle (can be used concurrently)
    pub async fn get_or_open(
        &self,
        file_id: fileid3,
        path: &str,
        sftp: &SFTPConnection,
    ) -> Result<SFTPHandle, nfsstat3> {
        let mut cache = self.handles.lock().await;

        // Check if we have a cached handle
        if let Some(cached) = cache.get_mut(&file_id) {
            debug!("Using cached handle for file {}", file_id);
            // Update last used time
            cached.last_used = Instant::now();
            return Ok(cached.handle.clone());
        }

        // Not cached - open new handle
        debug!("Opening new handle for file {} ({})", file_id, path);
        let handle = sftp
            .open(path, SFTPOpenFlags::WRITE)
            .await
            .map_err(|e| {
                log::error!("Failed to open file for handle cache: {}", e);
                nfsstat3::NFS3ERR_IO
            })?;

        // Cache it (without attributes initially)
        cache.insert(
            file_id,
            CachedHandle {
                handle: handle.clone(),
                last_used: Instant::now(),
                path: path.to_string(),
                cached_attrs: None,
                last_fstat: None,
            },
        );

        Ok(handle)
    }

    /// Check if we have fresh cached attributes
    ///
    /// Returns Some(attrs) if cached and fresh, None otherwise
    pub async fn get_cached_attrs(&self, file_id: fileid3) -> Option<FileAttributes> {
        let mut cache = self.handles.lock().await;
        let now = Instant::now();

        if let Some(cached) = cache.get_mut(&file_id) {
            if let (Some(attrs), Some(last_fstat)) = (&cached.cached_attrs, cached.last_fstat) {
                if now.duration_since(last_fstat) < self.attr_cache_ttl {
                    debug!(
                        "Using cached attributes for file {} (age: {:?})",
                        file_id,
                        now.duration_since(last_fstat)
                    );
                    cached.last_used = now;
                    return Some(attrs.clone());
                }
            }
        }
        None
    }

    /// Update cached attributes for a file
    pub async fn update_cached_attrs(&self, file_id: fileid3, attrs: FileAttributes) {
        let mut cache = self.handles.lock().await;
        let now = Instant::now();

        if let Some(cached) = cache.get_mut(&file_id) {
            debug!("Updating cached attributes for file {}", file_id);
            cached.cached_attrs = Some(attrs);
            cached.last_fstat = Some(now);
            cached.last_used = now;
        }
    }

    /// Invalidate cached attributes (but keep handle open)
    ///
    /// Called when attributes change (setattr, truncate, etc.)
    pub async fn invalidate_attrs(&self, file_id: fileid3) {
        let mut cache = self.handles.lock().await;

        if let Some(cached) = cache.get_mut(&file_id) {
            debug!("Invalidating cached attributes for file {}", file_id);
            cached.cached_attrs = None;
            cached.last_fstat = None;
        }
    }

    /// Close and remove idle handles
    ///
    /// Should be called periodically by a background task
    pub async fn cleanup_idle(&self, sftp: &SFTPConnection) {
        let mut cache = self.handles.lock().await;
        let now = Instant::now();

        // Find handles that have been idle too long
        let to_remove: Vec<_> = cache
            .iter()
            .filter(|(_, cached)| now.duration_since(cached.last_used) > self.idle_timeout)
            .map(|(id, _)| *id)
            .collect();

        // Close and remove them
        for file_id in to_remove {
            if let Some(cached) = cache.remove(&file_id) {
                info!(
                    "Closing idle handle for file {} ({})",
                    file_id, cached.path
                );
                // Best effort close - don't propagate errors
                let _ = sftp.close(cached.handle).await;
            }
        }
    }

    /// Invalidate (close) a specific cached handle
    ///
    /// Called when a file is removed or we need to ensure handle is closed
    pub async fn invalidate(&self, file_id: fileid3, sftp: &SFTPConnection) {
        let mut cache = self.handles.lock().await;
        if let Some(cached) = cache.remove(&file_id) {
            debug!("Invalidating cached handle for file {}", file_id);
            let _ = sftp.close(cached.handle).await;
        }
    }

    /// Close all cached handles
    ///
    /// Called during shutdown or SFTP reconnection
    pub async fn clear_all(&self, sftp: &SFTPConnection) {
        let mut cache = self.handles.lock().await;
        debug!("Closing all {} cached handles", cache.len());

        for (file_id, cached) in cache.drain() {
            debug!("Closing handle for file {} ({})", file_id, cached.path);
            let _ = sftp.close(cached.handle).await;
        }
    }

    /// Get number of cached handles (for debugging/monitoring)
    pub async fn size(&self) -> usize {
        self.handles.lock().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_creation() {
        let cache = HandleCache::new(Duration::from_secs(30), Duration::from_millis(100));
        assert_eq!(cache.idle_timeout, Duration::from_secs(30));
        assert_eq!(cache.attr_cache_ttl, Duration::from_millis(100));
    }
}
