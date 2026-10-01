//! Small CPU jobs over already-admitted chunks. Storage remains per chunk.

use tokio::sync::{OwnedSemaphorePermit, oneshot};

use crate::digest::ChunkId;

const MAX_CHUNKS: usize = 4;
const MAX_BYTES: usize = 1024 * 1024;

pub(super) struct Hashed {
    // Drop bytes before returning their admission on every cancellation path.
    pub data: Vec<u8>,
    pub digest: ChunkId,
    pub guard: OwnedSemaphorePermit,
}

struct Pending {
    data: Vec<u8>,
    guard: OwnedSemaphorePermit,
    result: oneshot::Sender<Hashed>,
}

#[derive(Default)]
pub(super) struct HashBatch {
    pending: Vec<Pending>,
    bytes: usize,
    cpu: Option<crate::import_cpu::ImportCpuBudget>,
}

impl HashBatch {
    pub fn new(cpu: Option<crate::import_cpu::ImportCpuBudget>) -> Self {
        Self {
            cpu,
            ..Self::default()
        }
    }

    pub fn push(
        &mut self,
        data: Vec<u8>,
        guard: OwnedSemaphorePermit,
    ) -> oneshot::Receiver<Hashed> {
        if self.bytes.saturating_add(data.len()) > MAX_BYTES {
            self.flush();
        }
        let (result, receiver) = oneshot::channel();
        self.bytes += data.len();
        self.pending.push(Pending {
            data,
            guard,
            result,
        });
        // An oversized chunk was already admitted by the shared byte budget;
        // run it alone rather than imposing an impossible second admission.
        if self.pending.len() == MAX_CHUNKS || self.bytes >= MAX_BYTES {
            self.flush();
        }
        receiver
    }

    pub fn flush(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let pending = std::mem::take(&mut self.pending);
        self.bytes = 0;
        match self.cpu.as_ref() {
            None => {
                tokio::task::spawn_blocking(move || hash(pending));
            }
            Some(cpu) => {
                if let Some(permit) = cpu.try_acquire() {
                    permit.spawn(move || hash(pending));
                } else {
                    let cpu = cpu.clone();
                    tokio::spawn(async move {
                        let mut pending = pending;
                        // Dropped writers must return queued byte admission even
                        // while another import occupies every CPU slot.
                        let permit = {
                            let abandoned = futures::future::join_all(
                                pending.iter_mut().map(|item| item.result.closed()),
                            );
                            tokio::select! {
                                permit = cpu.acquire() => permit,
                                _ = abandoned => return,
                            }
                        };
                        permit.spawn(move || hash(pending));
                    });
                }
            }
        }
    }
}

fn hash(pending: Vec<Pending>) {
    #[cfg(test)]
    super::hash_batch_tests::record_hash_job(pending.iter().map(|p| p.data.as_slice()));
    for item in pending {
        if item.result.is_closed() {
            continue;
        }
        let digest = ChunkId::new(blake3::hash(&item.data).into());
        // Sending never waits on storage or another blocking job. Bytes retain
        // their own admission; the CPU permit ends with this function.
        let _ = item.result.send(Hashed {
            data: item.data,
            digest,
            guard: item.guard,
        });
    }
}
