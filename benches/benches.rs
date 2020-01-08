use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use kvs::{KvStore, KvsEngine, Sled};
use rand::prelude::*;
use tempfile::TempDir;

pub fn benchmark(c: &mut Criterion) {
    let mut rng = thread_rng();

    macro_rules! get_key {
        () => {
            format!("key{}", rng.gen_range(0, 100))
        };
    }

    macro_rules! get_value {
        () => {
            "v".repeat(rng.gen_range(1, 100000))
        };
    }

    macro_rules! bench_write {
        ($engine:ident) => {
            let mut group = c.benchmark_group("write");
            let temp_dir = TempDir::new().unwrap();
            let mut store = $engine::open(temp_dir.path()).unwrap();
            group.bench_function(BenchmarkId::new(stringify!($engine), ""), |b| {
                b.iter_batched(
                    || (get_key!(), get_value!()),
                    |(key, value)| store.set(key, value).unwrap(),
                    BatchSize::SmallInput,
                )
            });
            group.finish();
        };
    }

    macro_rules! bench_read {
        ($engine:ident) => {
            let mut group = c.benchmark_group("read");
            let temp_dir = TempDir::new().unwrap();
            let mut store = $engine::open(temp_dir.path()).unwrap();
            (0..100).for_each(|_| {
                store.set(get_key!(), get_value!()).unwrap();
            });
            group.bench_function(BenchmarkId::new(stringify!($engine), ""), |b| {
                b.iter_batched(
                    || get_key!(),
                    |key| store.get(key).unwrap(),
                    BatchSize::SmallInput,
                )
            });
            group.finish();
        };
    }

    bench_write!(KvStore);
    bench_write!(Sled);
    bench_read!(KvStore);
    bench_read!(Sled);
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
