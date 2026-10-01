//! Partitioned admission for enumerated import producer buffers.

use std::future::Future;
use std::io;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(crate) const UNIT_BYTES: usize = 64 * 1024;
tokio::task_local! { static CURRENT: Option<ImportBufferBudget>; }

/// Shared, separate allowances for Git source and chunked-writer buffers.
///
/// Admission covers conservative envelopes for enumerated producer payload
/// buffers, not process RSS.
/// Gix workspaces/caches, verification buffers, manifest/index metadata, codec
/// workspace, allocator overhead/reallocation transients and backend-owned
/// payloads/caches are outside these allowances.
/// Encoded bytes transferred to object storage or pack staging become
/// backend-owned, including detached writes that outlive cancellation.
/// The partitions never borrow from one another, preserving destination room
/// while source bodies remain live. Disabled unless explicitly selected.
#[derive(Clone, Debug)]
pub struct ImportBufferBudget {
    pub(crate) source: Partition,
    pub(crate) destination: Partition,
}

impl ImportBufferBudget {
    /// Construct two non-borrowing partitions. Capacities round down to 64 KiB;
    /// reservations round up. Each partition must contain at least one unit.
    /// An impossible individual reservation fails instead of exceeding its cap.
    pub fn new(source_bytes: usize, destination_bytes: usize) -> io::Result<Self> {
        Ok(Self {
            source: Partition::new(source_bytes)?,
            destination: Partition::new(destination_bytes)?,
        })
    }

    /// Usable source capacity after rounding down.
    pub fn source_capacity(&self) -> usize {
        self.source.capacity()
    }

    /// Usable destination capacity after rounding down.
    pub fn destination_capacity(&self) -> usize {
        self.destination.capacity()
    }

    /// Currently reserved source envelopes across all clones.
    pub fn reserved_source_bytes(&self) -> usize {
        self.source.shared.reserved.load(Ordering::Relaxed)
    }

    /// Currently reserved writer envelopes across all clones.
    pub fn reserved_destination_bytes(&self) -> usize {
        self.destination.shared.reserved.load(Ordering::Relaxed)
    }

    /// Peak reserved source envelopes across all clones since construction.
    pub fn peak_source_bytes(&self) -> usize {
        self.source.shared.peak.load(Ordering::Relaxed)
    }

    /// Peak reserved writer envelopes across all clones since construction.
    pub fn peak_destination_bytes(&self) -> usize {
        self.destination.shared.peak.load(Ordering::Relaxed)
    }

    /// Apply admission to supported chunked writers opened while polling this
    /// future. Spawned tasks need explicit propagation. Git closure requests
    /// override the scope with their own selected budget, including none.
    pub async fn scope<F: Future>(&self, future: F) -> F::Output {
        scope(Some(self.clone()), future).await
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Partition {
    shared: Arc<Shared>,
    units: u32,
}
#[derive(Debug)]
struct Shared {
    slots: Arc<Semaphore>,
    reserved: AtomicUsize,
    peak: AtomicUsize,
}

impl Partition {
    pub(crate) fn new(bytes: usize) -> io::Result<Self> {
        let units = bytes / UNIT_BYTES;
        if units == 0 || units > Semaphore::MAX_PERMITS || units > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "import buffer partition must contain 1..=u32::MAX units of 64 KiB",
            ));
        }
        Ok(Self {
            shared: Arc::new(Shared {
                slots: Arc::new(Semaphore::new(units)),
                reserved: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
            }),
            units: units as u32,
        })
    }

    pub(crate) fn capacity(&self) -> usize {
        self.units as usize * UNIT_BYTES
    }

    fn units_for(&self, bytes: usize) -> io::Result<u32> {
        let units = bytes.div_ceil(UNIT_BYTES).max(1);
        if units > self.units as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "import buffer reservation of {bytes} bytes exceeds partition capacity {}",
                    self.capacity()
                ),
            ));
        }
        Ok(units as u32)
    }

    pub(crate) async fn reserve(&self, bytes: usize) -> io::Result<Arc<Reservation>> {
        let units = self.units_for(bytes)?;
        let permit = self
            .shared
            .slots
            .clone()
            .acquire_many_owned(units)
            .await
            .expect("private import buffer admission remains open");
        Ok(self.observe(permit, units))
    }

    #[cfg(test)]
    pub(crate) fn try_reserve(&self, bytes: usize) -> Option<Arc<Reservation>> {
        let units = self.units_for(bytes).ok()?;
        let permit = self
            .shared
            .slots
            .clone()
            .try_acquire_many_owned(units)
            .ok()?;
        Some(self.observe(permit, units))
    }

    fn observe(&self, permit: OwnedSemaphorePermit, units: u32) -> Arc<Reservation> {
        let bytes = units as usize * UNIT_BYTES;
        let reserved = self.shared.reserved.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.shared.peak.fetch_max(reserved, Ordering::Relaxed);
        Arc::new(Reservation {
            _permit: permit,
            shared: self.shared.clone(),
            bytes,
        })
    }
}

/// Keep this after the protected buffers in owning structs. A clone travels
/// through queued, running and completed-unreceived work independently of CPU
/// permits; no reservation belongs in a reusable source pool.
#[derive(Debug)]
pub(crate) struct Reservation {
    _permit: OwnedSemaphorePermit,
    shared: Arc<Shared>,
    bytes: usize,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.shared
            .reserved
            .fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

pub(crate) fn current() -> Option<ImportBufferBudget> {
    CURRENT.try_with(Clone::clone).ok().flatten()
}

pub(crate) async fn scope<F: Future>(budget: Option<ImportBufferBudget>, future: F) -> F::Output {
    if budget.is_none() && current().is_none() {
        future.await
    } else {
        CURRENT.scope(budget, future).await
    }
}
