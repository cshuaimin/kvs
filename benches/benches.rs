use async_std::task;
use criterion::{criterion_group, criterion_main, Criterion};
use kvs::KvStore;
use tempfile::TempDir;

pub fn get(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let store = task::block_on(async {
        let store = KvStore::open(temp_dir.path()).await.unwrap();
        store.set("key", "value").await.unwrap();
        store
    });
    c.bench_function("kvs", |b| {
        b.iter(|| {
            task::block_on(async {
                store.get("key").await.unwrap();
            })
        });
    });
}

pub fn set(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let store = task::block_on(KvStore::open(temp_dir.path())).unwrap();
    c.bench_function("kvs", |b| {
        b.iter(|| {
            task::block_on(async {
                store.set("key", "value").await.unwrap();
            })
        });
    });
}

criterion_group!(benches, get, set);
criterion_main!(benches);
