//! Compare the two object_store range entry points with identical bytes.
//! Local get_range opens/reads in separate blocking dispatches; get_ranges
//! can complete a singleton range in one dispatch. In-memory cases expose
//! the allocation overhead when there is no local-I/O dispatch to remove.
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use std::ops::Range;
use std::sync::Arc;

#[derive(Clone, Copy)]
enum Strategy { Range, SingletonRanges }

async fn pass(store: &dyn ObjectStore, path: &Path, range: Range<u64>, expected: &[u8], strategy: Strategy, concurrency: usize) {
    let reads = futures::stream::iter(0..64).map(|_| async {
        let bytes = match strategy {
            Strategy::Range => store.get_range(path, range.clone()).await.unwrap(),
            Strategy::SingletonRanges => {
                let mut ranges = store.get_ranges(path, std::slice::from_ref(&range)).await.unwrap();
                assert_eq!(ranges.len(), 1);
                ranges.pop().unwrap()
            }
        };
        assert_eq!(bytes.as_ref(), expected);
    }).buffer_unordered(concurrency);
    futures::pin_mut!(reads);
    while reads.next().await.is_some() {}
}

fn local_range_read(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
    let mut group = c.benchmark_group("object_store_single_range");
    group.sample_size(10);
    let path = Path::from("pack");
    for size in [1, 128, 4096, 65535, 65536, 65537, 1048576] {
        let scratch = tempfile::tempdir().unwrap();
        let mut payload = vec![0; size + 17];
        blake3::Hasher::new().update(b"single-range-fixture").finalize_xof().fill(&mut payload);
        let stores: [(&str, Arc<dyn ObjectStore>); 2] = [
            ("local", Arc::new(object_store::local::LocalFileSystem::new_with_prefix(scratch.path()).unwrap())),
            ("memory", Arc::new(object_store::memory::InMemory::new())),
        ];
        for (backend, store) in stores {
            runtime.block_on(store.put(&path, payload.clone().into())).unwrap();
            let range = 17..payload.len() as u64;
            let expected = &payload[17..];
            for concurrency in [1, 16] {
                for (name, strategy) in [("range", Strategy::Range), ("singleton-ranges", Strategy::SingletonRanges)] {
                    runtime.block_on(pass(store.as_ref(), &path, range.clone(), expected, strategy, concurrency));
                    group.throughput(Throughput::Bytes((size * 64) as u64));
                    group.bench_function(BenchmarkId::new(format!("{backend}/{name}/concurrency-{concurrency}"), size), |b| {
                        b.to_async(&runtime).iter(|| pass(store.as_ref(), &path, range.clone(), expected, strategy, concurrency));
                    });
                }
            }
        }
    }
    group.finish();
}
criterion_group!(benches, local_range_read);
criterion_main!(benches);
