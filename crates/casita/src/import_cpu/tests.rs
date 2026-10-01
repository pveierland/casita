use std::num::NonZeroUsize;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use tokio::sync::oneshot;

use super as import_cpu;
use super::ImportCpuBudget;

struct Release(Option<std::sync::mpsc::Sender<()>>);
impl Drop for Release {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

fn runtime(blocking: usize) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(blocking)
        .enable_all()
        .build()
        .unwrap()
}

#[test]
fn clones_share_execution_admission() {
    runtime(2).block_on(async {
        let budget = ImportCpuBudget::new(NonZeroUsize::MIN);
        let first_budget = budget.clone();
        let (entered, started) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let release = Release(Some(release));
        let first = tokio::spawn(async move {
            first_budget
                .run(move || {
                    entered.send(()).unwrap();
                    wait.recv().unwrap();
                })
                .await
                .unwrap();
        });
        started.await.unwrap();
        let ran = Arc::new(AtomicBool::new(false));
        let observed = ran.clone();
        let second = tokio::spawn(async move {
            budget
                .run(move || {
                    observed.store(true, Ordering::SeqCst);
                })
                .await
                .unwrap();
        });
        // The first real blocking job stays parked while the other runtime
        // thread is available. A missing/shared-by-value gate admits the clone.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let overlapped = ran.load(Ordering::SeqCst);
        drop(release);
        first.await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), second)
            .await
            .unwrap()
            .unwrap();
        assert!(
            !overlapped,
            "a clone executed while the single CPU slot was occupied"
        );
        assert!(ran.load(Ordering::SeqCst));
    });
}

#[test]
fn completed_unreceived_output_does_not_hold_execution_admission() {
    runtime(1).block_on(async {
        let budget = ImportCpuBudget::new(NonZeroUsize::MIN);
        let (entered, completed) = oneshot::channel();
        let first = budget.run(move || {
            entered.send(()).unwrap();
            vec![31; 65536]
        });
        futures::pin_mut!(first);
        assert!(futures::poll!(first.as_mut()).is_pending());
        completed.await.unwrap();
        // Do not poll first again until another admitted CPU job has completed.
        let answer = tokio::time::timeout(Duration::from_secs(3), budget.run(|| 19))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(answer, 19);
        assert_eq!(first.await.unwrap(), vec![31; 65536]);
    });
}

#[test]
fn cancelled_waiter_releases_its_captured_buffers() {
    runtime(2).block_on(async {
        let budget = ImportCpuBudget::new(NonZeroUsize::MIN);
        let first_budget = budget.clone();
        let (entered, started) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let release = Release(Some(release));
        let first = tokio::spawn(async move {
            first_budget
                .run(move || {
                    entered.send(()).unwrap();
                    wait.recv().unwrap();
                })
                .await
                .unwrap();
        });
        started.await.unwrap();
        let buffer = Arc::new(vec![47; 65536]);
        let weak = Arc::downgrade(&buffer);
        let ran = Arc::new(AtomicBool::new(false));
        let observed = ran.clone();
        let waiting = budget.run(move || {
            observed.store(true, Ordering::SeqCst);
            buffer
        });
        let mut waiting = Box::pin(waiting);
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        drop(waiting);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let executed_after_cancellation = ran.load(Ordering::SeqCst);
        let retained_after_cancellation = weak.upgrade().is_some();
        drop(release);
        first.await.unwrap();
        assert!(
            !executed_after_cancellation,
            "cancelled admission waiter executed its work"
        );
        assert!(
            !retained_after_cancellation,
            "cancelled waiter retained its input buffer"
        );
        assert_eq!(budget.run(|| 23).await.unwrap(), 23);
    });
}

#[test]
fn dropped_source_dispatcher_returns_waiting_private_slot_and_buffers() {
    runtime(2).block_on(async {
        let budget = ImportCpuBudget::new(NonZeroUsize::MIN);
        let occupied = budget.clone();
        let (entered, started) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let release = Release(Some(release));
        let foreign = tokio::spawn(async move {
            occupied
                .run(move || {
                    let _ = entered.send(());
                    wait.recv().unwrap();
                })
                .await
                .unwrap();
        });
        started.await.unwrap();
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = slots.clone().acquire_owned().await.unwrap();
        let bytes = Arc::new(vec![37; 65536]);
        let weak = Arc::downgrade(&bytes);
        let dispatcher = import_cpu::spawn(Some(budget.clone()), move || {
            drop(bytes);
            drop(permit);
        });
        tokio::task::yield_now().await;
        drop(dispatcher);
        let recovered = tokio::time::timeout(Duration::from_secs(3), slots.acquire()).await;
        let freed = weak.upgrade().is_none();
        drop(release);
        foreign.await.unwrap();
        assert!(
            recovered.is_ok(),
            "abandoned dispatcher kept private source admission"
        );
        assert!(
            freed,
            "abandoned dispatcher kept its input behind foreign CPU work"
        );
        assert_eq!(budget.peak_jobs(), 1);
    });
}

#[test]
fn dropped_submitted_dispatcher_retains_private_slot_until_work_ends() {
    runtime(1).block_on(async {
        let budget = ImportCpuBudget::new(NonZeroUsize::MIN);
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = slots.clone().acquire_owned().await.unwrap();
        let bytes = Arc::new(vec![41; 65536]);
        let weak = Arc::downgrade(&bytes);
        let (entered, started) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let release = Release(Some(release));
        let dispatcher = import_cpu::spawn(Some(budget.clone()), move || {
            let _ = entered.send(());
            wait.recv().unwrap();
            drop(bytes);
            drop(permit);
        });
        started.await.unwrap();
        drop(dispatcher);
        tokio::task::yield_now().await;
        let premature = slots.try_acquire().is_ok();
        let retained = weak.upgrade().is_some();
        drop(release);
        let _drained = tokio::time::timeout(Duration::from_secs(3), slots.acquire())
            .await
            .unwrap()
            .unwrap();
        assert!(!premature, "submitted work lost private source admission");
        assert!(retained, "submitted work lost its captured input");
        assert!(weak.upgrade().is_none());
        assert_eq!(budget.run(|| 29).await.unwrap(), 29);
    });
}

#[test]
fn contended_cpu_dispatch_progresses_without_polling_its_caller_again() {
    runtime(1).block_on(async {
        let budget = ImportCpuBudget::new(NonZeroUsize::MIN);
        let occupied = budget.clone();
        let (entered, started) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let release = Release(Some(release));
        let foreign = tokio::spawn(async move {
            occupied
                .run(move || {
                    let _ = entered.send(());
                    wait.recv().unwrap();
                })
                .await
                .unwrap();
        });
        started.await.unwrap();
        // A duplex writer can stop polling its compression future while the
        // producer waits for another source step on this same CPU coordinator.
        let waiting = budget.run(|| vec![53; 65536]);
        futures::pin_mut!(waiting);
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        tokio::task::yield_now().await;
        drop(release);
        foreign.await.unwrap();
        // Do not poll waiting: assigning it a semaphore permit is insufficient
        // if it requires another caller poll to submit and release its job.
        let following = tokio::time::timeout(Duration::from_secs(1), budget.run(|| 59)).await;
        let output = waiting.await.unwrap();
        assert_eq!(output, vec![53; 65536]);
        assert_eq!(
            following
                .expect("unpolled admission waiter stranded shared CPU capacity")
                .unwrap(),
            59
        );
    });
}
