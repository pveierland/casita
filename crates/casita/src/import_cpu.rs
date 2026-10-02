//! Shared admission for selected import CPU jobs.

use std::{
    future::Future,
    io,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

tokio::task_local! { static CURRENT: Option<ImportCpuBudget>; }

/// Cloneable admission shared by selected imports and their chunk writers.
/// Limits blocking decode, hash and compression jobs. Inline chunking, Bao
/// hashing, asynchronous verification and storage are outside this limit.
#[derive(Debug, Clone)]
pub struct ImportCpuBudget {
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    slots: Arc<Semaphore>,
    active: AtomicUsize,
    peak: AtomicUsize,
}

impl ImportCpuBudget {
    /// Construct a shared limit. Clones participate in the same admission.
    pub fn new(jobs: NonZeroUsize) -> Self {
        Self {
            shared: Arc::new(Shared {
                slots: Arc::new(Semaphore::new(jobs.get().min(Semaphore::MAX_PERMITS))),
                active: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
            }),
        }
    }

    /// Peak executing blocking jobs across all clones since construction.
    pub fn peak_jobs(&self) -> usize {
        self.shared.peak.load(Ordering::Relaxed)
    }

    /// Apply admission to supported chunk writers opened while polling `future`.
    /// Spawned tasks do not inherit this scope. Pass a clone and scope their
    /// future explicitly when a custom backend dispatches writer creation.
    /// Git closure requests override this scope with their own selected budget,
    /// including no budget when the request is unconfigured.
    pub async fn scope<F: Future>(&self, future: F) -> F::Output {
        scope(Some(self.clone()), future).await
    }

    pub(crate) fn try_acquire(&self) -> Option<Permit> {
        self.shared
            .slots
            .clone()
            .try_acquire_owned()
            .ok()
            .map(|slot| Permit {
                _slot: slot,
                shared: self.shared.clone(),
            })
    }

    pub(crate) async fn acquire(&self) -> Permit {
        Permit {
            _slot: self
                .shared
                .slots
                .clone()
                .acquire_owned()
                .await
                .expect("private import CPU admission remains open"),
            shared: self.shared.clone(),
        }
    }

    pub(crate) async fn run<F, T>(&self, work: F) -> io::Result<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        spawn(Some(self.clone()), work).join().await
    }
}

pub(crate) struct Permit {
    _slot: OwnedSemaphorePermit,
    shared: Arc<Shared>,
}
struct Active(Arc<Shared>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::Relaxed);
    }
}
impl Permit {
    pub(crate) fn spawn<F, T>(self, work: F) -> tokio::task::JoinHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        tokio::task::spawn_blocking(move || {
            let active = self.shared.active.fetch_add(1, Ordering::Relaxed) + 1;
            self.shared.peak.fetch_max(active, Ordering::Relaxed);
            let active = Active(self.shared.clone());
            let result = work();
            // Neither CPU accounting nor its permit follows output into storage.
            drop(active);
            drop(self);
            result
        })
    }
}

pub(crate) fn current() -> Option<ImportCpuBudget> {
    CURRENT.try_with(Clone::clone).ok().flatten()
}

pub(crate) async fn scope<F: Future>(budget: Option<ImportCpuBudget>, future: F) -> F::Output {
    // An unconfigured import masks an enclosing scope, rather than accidentally
    // joining a coordinator it never selected. Ordinary callers avoid task-local setup.
    if budget.is_none() && current().is_none() {
        future.await
    } else {
        CURRENT.scope(budget, future).await
    }
}

pub(crate) async fn run<F, T>(budget: Option<&ImportCpuBudget>, work: F) -> io::Result<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    match budget {
        Some(budget) => budget.run(work).await,
        None => tokio::task::spawn_blocking(work)
            .await
            .map_err(io::Error::other),
    }
}

/// Eager CPU dispatcher. Only outer async admission tasks are abortable;
/// submitted blocking work retains its buffers and private admission guards.
pub(crate) struct Task<T> {
    handle: tokio::task::JoinHandle<io::Result<T>>,
    abort_waiter: bool,
}
impl<T> Task<T> {
    pub(crate) fn cancel_waiter(&self) {
        if self.abort_waiter {
            self.handle.abort();
        }
    }
    pub(crate) async fn join(mut self) -> io::Result<T> {
        (&mut self.handle).await.map_err(io::Error::other)?
    }
}
impl<T> Drop for Task<T> {
    fn drop(&mut self) {
        self.cancel_waiter();
    }
}

pub(crate) fn spawn<F, T>(budget: Option<ImportCpuBudget>, work: F) -> Task<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    match budget {
        Some(budget) => {
            if let Some(permit) = budget.try_acquire() {
                Task {
                    handle: permit.spawn(move || Ok(work())),
                    abort_waiter: false,
                }
            } else {
                // A semaphore can assign capacity before its waiter is polled.
                // Advance independently: a duplex writer may stop polling this
                // compression future while its source needs the same CPU slot.
                Task {
                    handle: tokio::spawn(async move {
                        budget
                            .acquire()
                            .await
                            .spawn(work)
                            .await
                            .map_err(io::Error::other)
                    }),
                    abort_waiter: true,
                }
            }
        }
        None => Task {
            handle: tokio::task::spawn_blocking(move || Ok(work())),
            abort_waiter: false,
        },
    }
}

#[cfg(all(test, feature = "native"))]
mod tests;
