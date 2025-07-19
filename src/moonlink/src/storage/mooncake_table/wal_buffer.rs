use crate::table_notify::TableEvent;
use tokio::sync::Mutex;

/// WalBuffer tracks both the in-memory WAL and the flushed WALs.
/// The in-memory WAL may be modified concurrently, but tracking persistent WAL
/// is only done during the persist operation (and may include cleaning up old WALs).
pub struct WalBuffer {
    /// The in_mem_wal could have insertions done by incoming CDC events, as well as removed by the flush operation
    in_mem_wal: Mutex<Vec<TableEvent>>,
    /// The wal file numbers that are still live. The wal file numbers
    /// track the highest LSN of the WAL file.
    /// Mutex is not technically needed here because only one persist operation is allowed at a time,
    /// but since we pass WalBuffer to multiple threads, we use a mutex.
    live_wal_file_tracker: Mutex<Vec<u64>>,

    /// For testing:
    flushed_wals: Mutex<Vec<Vec<TableEvent>>>,
}

impl WalBuffer {
    pub fn new() -> Self {
        Self {
            in_mem_wal: Mutex::new(Vec::new()),
            live_wal_file_tracker: Mutex::new(Vec::new()),
            flushed_wals: Mutex::new(Vec::new()),
        }
    }

    pub async fn insert(&self, row: TableEvent) {
        let mut buf = self.in_mem_wal.lock().await;
        buf.push(row);
    }

    async fn take_and_replace(&self) -> Option<Vec<TableEvent>> {
        let mut buf = self.in_mem_wal.lock().await;
        if buf.is_empty() {
            return None;
        }
        let mut new_buf = Vec::new();
        std::mem::swap(&mut *buf, &mut new_buf);
        Some(new_buf)
    }

    pub async fn persist_to_new_file(&self) {
        let old_wal = self.take_and_replace().await;

        if let Some(old_wal) = old_wal {
            // get the highest LSN of the old wal
            assert!(old_wal.len() > 0);
            // TODO(Paul): Need to handle stream flush since that one has no LSN
            let highest_lsn = old_wal[old_wal.len() - 1]
                .get_lsn_for_ingest_event()
                .unwrap();

            // update latest flushed WAL number
            self.live_wal_file_tracker.lock().await.push(highest_lsn);

            // TODO(Paul): Implement persistent WAL
            self.flushed_wals.lock().await.push(old_wal);
        }
    }

    // TODO(Paul): Implement persistent WAL
    pub async fn truncate_flushed_wals(&self, truncate_from_lsn: u64) {
        let mut flushed_wals = self.flushed_wals.lock().await;
        let mut live_wal_file_tracker = self.live_wal_file_tracker.lock().await;
        flushed_wals.retain(|wal| {
            wal.last().unwrap().get_lsn_for_ingest_event().unwrap() >= truncate_from_lsn
        });
        live_wal_file_tracker.retain(|lsn| *lsn >= truncate_from_lsn);

        assert!(live_wal_file_tracker.len() == flushed_wals.len());
    }

    // TODO(Paul): Implement persistent WAL
    pub async fn get_flushed_wal(&self, replay_from_lsn: u64) -> Vec<TableEvent> {
        let mut result = Vec::new();

        let mut seen_first_relevant_file = false;
        for wal in self.flushed_wals.lock().await.iter() {
            // First, filter out files that would be way before the replay_from_lsn
            if !seen_first_relevant_file {
                if wal.last().unwrap().get_lsn_for_ingest_event().unwrap() >= replay_from_lsn {
                    seen_first_relevant_file = true;
                }
            }
            if seen_first_relevant_file {
                for event in wal.iter() {
                    if event.get_lsn_for_ingest_event().unwrap() >= replay_from_lsn {
                        result.push(event.clone_for_wal_buffer());
                        break;
                    }
                }
            }
        }
        result
    }
}
