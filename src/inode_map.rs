//! Bidirectional mapping between NFS file IDs (inodes) and SFTP paths
//!
//! NFS uses numeric file IDs to identify files, while SFTP uses string paths.
//! This module provides efficient bidirectional translation with caching and
//! lifecycle management.

use nfsserve::nfs::fileid3;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

/// Bidirectional map between NFS inodes (fileid3) and SFTP paths
///
/// Thread-safe structure for translating between NFS numeric file IDs
/// and SFTP string paths. Supports:
/// - Fast bidirectional lookup
/// - Monotonic inode allocation
/// - Pinning (preventing eviction of open files)
/// - Optional cache eviction (LRU)
pub struct InodeMap {
    /// Path -> Inode mapping
    path_to_inode: RwLock<HashMap<String, fileid3>>,

    /// Inode -> Path mapping (reverse index)
    inode_to_path: RwLock<HashMap<fileid3, String>>,

    /// Monotonic counter for allocating new inodes
    /// Starts at 1 (0 is reserved for root)
    next_inode: AtomicU64,

    /// Set of pinned inodes (open files that can't be evicted)
    pinned: RwLock<HashSet<fileid3>>,

    /// Maximum cache size before eviction (0 = unlimited)
    max_cache_size: usize,
}

impl InodeMap {
    /// Create a new inode map with the given root path
    ///
    /// The root path is assigned inode 0 (the NFS root directory)
    ///
    /// # Arguments
    /// * `root_path` - The SFTP path to treat as the filesystem root
    /// * `max_cache_size` - Maximum number of entries (0 for unlimited)
    pub fn new(root_path: String, max_cache_size: usize) -> Self {
        let mut path_to_inode = HashMap::new();
        let mut inode_to_path = HashMap::new();

        // Root directory is always inode 0
        path_to_inode.insert(root_path.clone(), 0);
        inode_to_path.insert(0, root_path);

        Self {
            path_to_inode: RwLock::new(path_to_inode),
            inode_to_path: RwLock::new(inode_to_path),
            next_inode: AtomicU64::new(1), // Start at 1, 0 is root
            pinned: RwLock::new(HashSet::new()),
            max_cache_size,
        }
    }

    /// Get the inode for a path, allocating a new one if necessary
    ///
    /// If the path is already cached, returns the existing inode.
    /// Otherwise, allocates a new inode and adds the mapping.
    ///
    /// # Arguments
    /// * `path` - The SFTP path
    ///
    /// # Returns
    /// The file ID (inode) for this path
    pub fn get_or_allocate(&self, path: &str) -> fileid3 {
        // Fast path: check if already exists (read lock)
        {
            let path_map = self.path_to_inode.read().unwrap();
            if let Some(&inode) = path_map.get(path) {
                return inode;
            }
        }

        // Slow path: allocate new inode (write lock)
        let mut path_map = self.path_to_inode.write().unwrap();
        let mut inode_map = self.inode_to_path.write().unwrap();

        // Double-check in case another thread allocated it
        if let Some(&inode) = path_map.get(path) {
            return inode;
        }

        // Allocate new inode
        let inode = self.next_inode.fetch_add(1, Ordering::SeqCst);

        path_map.insert(path.to_string(), inode);
        inode_map.insert(inode, path.to_string());

        // TODO: Check cache size and evict if needed
        if self.max_cache_size > 0 && path_map.len() > self.max_cache_size {
            // For now, just log a warning
            // Future: implement LRU eviction
            log::warn!(
                "Inode cache size ({}) exceeds limit ({})",
                path_map.len(),
                self.max_cache_size
            );
        }

        inode
    }

    /// Get the path for an inode
    ///
    /// # Arguments
    /// * `inode` - The file ID to look up
    ///
    /// # Returns
    /// The SFTP path, or None if the inode is not in the cache
    pub fn get_path(&self, inode: fileid3) -> Option<String> {
        let inode_map = self.inode_to_path.read().unwrap();
        inode_map.get(&inode).cloned()
    }

    /// Get the inode for a path (if it exists in cache)
    ///
    /// Unlike `get_or_allocate`, this does not allocate a new inode.
    ///
    /// # Arguments
    /// * `path` - The SFTP path to look up
    ///
    /// # Returns
    /// The file ID if cached, or None if not in cache
    pub fn get_inode(&self, path: &str) -> Option<fileid3> {
        let path_map = self.path_to_inode.read().unwrap();
        path_map.get(path).copied()
    }

    /// Pin an inode (mark it as in-use, preventing eviction)
    ///
    /// Call this when opening a file handle to ensure the inode
    /// mapping isn't evicted while the file is open.
    ///
    /// # Arguments
    /// * `inode` - The file ID to pin
    pub fn pin(&self, inode: fileid3) {
        let mut pinned = self.pinned.write().unwrap();
        pinned.insert(inode);
    }

    /// Unpin an inode (allow it to be evicted again)
    ///
    /// Call this when closing a file handle.
    ///
    /// # Arguments
    /// * `inode` - The file ID to unpin
    pub fn unpin(&self, inode: fileid3) {
        let mut pinned = self.pinned.write().unwrap();
        pinned.remove(&inode);
    }

    /// Check if an inode is pinned
    ///
    /// # Arguments
    /// * `inode` - The file ID to check
    ///
    /// # Returns
    /// True if the inode is pinned (in-use)
    pub fn is_pinned(&self, inode: fileid3) -> bool {
        let pinned = self.pinned.read().unwrap();
        pinned.contains(&inode)
    }

    /// Update the path for an existing inode (for rename operations)
    ///
    /// # Arguments
    /// * `inode` - The file ID to update
    /// * `new_path` - The new SFTP path
    ///
    /// # Returns
    /// True if the inode was found and updated, false otherwise
    pub fn update_path(&self, inode: fileid3, new_path: &str) -> bool {
        let mut path_map = self.path_to_inode.write().unwrap();
        let mut inode_map = self.inode_to_path.write().unwrap();

        // Get old path and remove it
        if let Some(old_path) = inode_map.get(&inode) {
            path_map.remove(old_path);

            // Insert new mapping
            path_map.insert(new_path.to_string(), inode);
            inode_map.insert(inode, new_path.to_string());

            true
        } else {
            false
        }
    }

    /// Remove an inode from the cache (for delete operations)
    ///
    /// Pinned inodes cannot be removed.
    ///
    /// # Arguments
    /// * `inode` - The file ID to remove
    ///
    /// # Returns
    /// True if removed, false if not found or pinned
    pub fn remove(&self, inode: fileid3) -> bool {
        // Don't remove if pinned
        if self.is_pinned(inode) {
            return false;
        }

        let mut path_map = self.path_to_inode.write().unwrap();
        let mut inode_map = self.inode_to_path.write().unwrap();

        if let Some(path) = inode_map.remove(&inode) {
            path_map.remove(&path);
            true
        } else {
            false
        }
    }

    /// Get cache statistics
    ///
    /// # Returns
    /// (total_entries, pinned_count, next_inode)
    pub fn stats(&self) -> (usize, usize, u64) {
        let path_map = self.path_to_inode.read().unwrap();
        let pinned = self.pinned.read().unwrap();
        let next = self.next_inode.load(Ordering::Relaxed);

        (path_map.len(), pinned.len(), next)
    }

    /// Clear all mappings (except root)
    ///
    /// Useful for unmount or cache reset. Pinned entries are preserved.
    pub fn clear(&self) {
        let mut path_map = self.path_to_inode.write().unwrap();
        let mut inode_map = self.inode_to_path.write().unwrap();
        let pinned = self.pinned.read().unwrap();

        // Get root path before clearing
        let root_path = inode_map.get(&0).cloned();

        // Keep only root and pinned entries
        path_map.retain(|_path, inode| {
            *inode == 0 || pinned.contains(inode)
        });

        inode_map.retain(|inode, _path| {
            *inode == 0 || pinned.contains(inode)
        });

        // Ensure root is still there
        if let Some(root) = root_path {
            path_map.entry(root.clone()).or_insert(0);
            inode_map.entry(0).or_insert(root);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_root_allocation() {
        let map = InodeMap::new("/home/user".to_string(), 0);

        // Root should be inode 0
        assert_eq!(map.get_inode("/home/user"), Some(0));
        assert_eq!(map.get_path(0), Some("/home/user".to_string()));
    }

    #[test]
    fn test_allocation() {
        let map = InodeMap::new("/root".to_string(), 0);

        // Allocate some paths
        let inode1 = map.get_or_allocate("/root/file1.txt");
        let inode2 = map.get_or_allocate("/root/file2.txt");
        let inode3 = map.get_or_allocate("/root/dir/file3.txt");

        // Should get monotonic inodes
        assert_eq!(inode1, 1);
        assert_eq!(inode2, 2);
        assert_eq!(inode3, 3);

        // Should be bidirectional
        assert_eq!(map.get_path(1), Some("/root/file1.txt".to_string()));
        assert_eq!(map.get_path(2), Some("/root/file2.txt".to_string()));
        assert_eq!(map.get_inode("/root/file1.txt"), Some(1));
    }

    #[test]
    fn test_idempotent_allocation() {
        let map = InodeMap::new("/root".to_string(), 0);

        // Allocating same path multiple times should return same inode
        let inode1 = map.get_or_allocate("/root/file.txt");
        let inode2 = map.get_or_allocate("/root/file.txt");
        let inode3 = map.get_or_allocate("/root/file.txt");

        assert_eq!(inode1, inode2);
        assert_eq!(inode2, inode3);
    }

    #[test]
    fn test_pinning() {
        let map = InodeMap::new("/root".to_string(), 0);
        let inode = map.get_or_allocate("/root/file.txt");

        assert!(!map.is_pinned(inode));

        map.pin(inode);
        assert!(map.is_pinned(inode));

        map.unpin(inode);
        assert!(!map.is_pinned(inode));
    }

    #[test]
    fn test_update_path() {
        let map = InodeMap::new("/root".to_string(), 0);
        let inode = map.get_or_allocate("/root/old.txt");

        // Update path
        assert!(map.update_path(inode, "/root/new.txt"));

        // Old path should not resolve
        assert_eq!(map.get_inode("/root/old.txt"), None);

        // New path should resolve to same inode
        assert_eq!(map.get_inode("/root/new.txt"), Some(inode));
        assert_eq!(map.get_path(inode), Some("/root/new.txt".to_string()));
    }

    #[test]
    fn test_remove() {
        let map = InodeMap::new("/root".to_string(), 0);
        let inode = map.get_or_allocate("/root/file.txt");

        // Remove unpinned inode
        assert!(map.remove(inode));
        assert_eq!(map.get_path(inode), None);
        assert_eq!(map.get_inode("/root/file.txt"), None);

        // Try to remove again - should fail
        assert!(!map.remove(inode));
    }

    #[test]
    fn test_remove_pinned() {
        let map = InodeMap::new("/root".to_string(), 0);
        let inode = map.get_or_allocate("/root/file.txt");

        map.pin(inode);

        // Can't remove pinned inode
        assert!(!map.remove(inode));
        assert_eq!(map.get_path(inode), Some("/root/file.txt".to_string()));
    }

    #[test]
    fn test_clear() {
        let map = InodeMap::new("/root".to_string(), 0);

        let inode1 = map.get_or_allocate("/root/file1.txt");
        let inode2 = map.get_or_allocate("/root/file2.txt");
        let inode3 = map.get_or_allocate("/root/file3.txt");

        // Pin one of them
        map.pin(inode2);

        map.clear();

        // Root should still exist
        assert_eq!(map.get_path(0), Some("/root".to_string()));

        // Unpinned should be gone
        assert_eq!(map.get_path(inode1), None);
        assert_eq!(map.get_path(inode3), None);

        // Pinned should still exist
        assert_eq!(map.get_path(inode2), Some("/root/file2.txt".to_string()));
    }

    #[test]
    fn test_stats() {
        let map = InodeMap::new("/root".to_string(), 0);

        map.get_or_allocate("/root/file1.txt");
        let inode2 = map.get_or_allocate("/root/file2.txt");
        map.get_or_allocate("/root/file3.txt");

        map.pin(inode2);

        let (total, pinned, next) = map.stats();

        assert_eq!(total, 4); // root + 3 files
        assert_eq!(pinned, 1);
        assert_eq!(next, 4); // Next inode to allocate
    }
}
