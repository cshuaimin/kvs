use async_std::sync::Arc;
use async_std::task;
use tempfile::TempDir;
// use walkdir::WalkDir;

use kvs::{KvStore, Result};

// Should get previously stored value
#[test]
fn get_stored_value() -> Result<()> {
    task::block_on(async {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let store = KvStore::open(temp_dir.path()).await?;

        store.set("key1", "value1").await?;
        store.set("key2", "value2").await?;

        assert_eq!(store.get("key1").await?, Some(b"value1".to_vec()));
        assert_eq!(store.get("key2").await?, Some(b"value2".to_vec()));

        // Open from disk again and check persistent data
        drop(store);
        let store = KvStore::open(temp_dir.path()).await?;
        assert_eq!(store.get("key1").await?, Some(b"value1".to_vec()));
        assert_eq!(store.get("key2").await?, Some(b"value2".to_vec()));
        Ok(())
    })
}

// Should overwrite existent value
#[test]
fn overwrite_value() -> Result<()> {
    task::block_on(async {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let store = KvStore::open(temp_dir.path()).await?;

        store.set("key1", "value1").await?;
        assert_eq!(store.get("key1").await?, Some(b"value1".to_vec()));
        store.set("key1", "value2").await?;
        assert_eq!(store.get("key1").await?, Some(b"value2".to_vec()));

        // Open from disk again and check persistent data
        drop(store);
        let store = KvStore::open(temp_dir.path()).await?;
        assert_eq!(store.get("key1").await?, Some(b"value2".to_vec()));
        store.set("key1", "value3").await?;
        assert_eq!(store.get("key1").await?, Some(b"value3".to_vec()));
        Ok(())
    })
}

// Should get `None` when getting a non-existent key
#[test]
fn get_non_existent_value() -> Result<()> {
    task::block_on(async {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let store = KvStore::open(temp_dir.path()).await?;

        store.set("key1", "value1").await?;
        assert_eq!(store.get("key2").await?, None);

        // Open from disk again and check persistent data
        drop(store);
        let store = KvStore::open(temp_dir.path()).await?;
        assert_eq!(store.get("key2").await?, None);
        Ok(())
    })
}

#[test]
fn remove_non_existent_key() -> Result<()> {
    task::block_on(async {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let store = KvStore::open(temp_dir.path()).await?;
        assert!(store.remove("key1").await.is_err());
        Ok(())
    })
}

#[test]
fn remove_key() -> Result<()> {
    task::block_on(async {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let store = KvStore::open(temp_dir.path()).await?;
        store.set("key1", "value1").await?;
        assert!(store.remove("key1").await.is_ok());
        assert_eq!(store.get("key1").await?, None);
        Ok(())
    })
}

// Insert data until total size of the directory decreases.
// Test data correctness after compaction.
// #[test]
// fn compaction() -> Result<()> {
//     let temp_dir = TempDir::new().expect("unable to create temporary working directory");
//     let store = KvStore::open(temp_dir.path()).await?;

//     let dir_size = || {
//         let entries = WalkDir::new(temp_dir.path()).into_iter();
//         let len: walkdir::Result<u64> = entries
//             .map(|res| {
//                 res.and_then(|entry| entry.metadata())
//                     .map(|metadata| metadata.len())
//             })
//             .sum();
//         len.expect("fail to get directory size")
//     };

//     let mut current_size = dir_size();
//     for iter in 0..1000 {
//         for key_id in 0..1000 {
//             let key = format!("key{}", key_id);
//             let value = format!("{}", iter);
//             store.set(key, value)?;
//         }

//         let new_size = dir_size();
//         if new_size > current_size {
//             current_size = new_size;
//             continue;
//         }
//         // Compaction triggered

//         drop(store);
//         // reopen and check content
//         let store = KvStore::open(temp_dir.path()).await?;
//         for key_id in 0..1000 {
//             let key = format!("key{}", key_id);
//             assert_eq!(store.get(key)?, Some(format!("{}", iter)));
//         }
//         return Ok(());
//     }

//     panic!("No compaction detected");
// }

#[test]
fn concurrent_set() -> Result<()> {
    task::block_on(async {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let store = Arc::new(KvStore::open(temp_dir.path()).await?);
        const N: usize = 1000;
        let mut tasks = Vec::with_capacity(N);
        for i in 0..N {
            let store = Arc::clone(&store);
            tasks.push(task::spawn(async move {
                store.set(format!("key{}", i), format!("value{}", i)).await
            }));
        }
        for task in tasks {
            task.await?;
        }

        for i in 0..N {
            assert_eq!(
                store.get(format!("key{}", i)).await?,
                Some(format!("value{}", i).into_bytes())
            );
        }

        // Open from disk again and check persistent data
        drop(store);
        let store = KvStore::open(temp_dir.path()).await?;
        for i in 0..N {
            assert_eq!(
                store.get(format!("key{}", i)).await?,
                Some(format!("value{}", i).into_bytes())
            );
        }
        Ok(())
    })
}

#[test]
fn concurrent_get() -> Result<()> {
    task::block_on(async {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let store = Arc::new(KvStore::open(temp_dir.path()).await?);
        for i in 0..100 {
            store
                .set(format!("key{}", i), format!("value{}", i))
                .await?
        }

        let mut tasks = Vec::with_capacity(100);
        for id in 0..100 {
            let store = Arc::clone(&store);
            tasks.push(task::spawn(async move {
                for i in 0..100 {
                    let key_id = (i + id) % 100;
                    assert_eq!(
                        store.get(format!("key{}", key_id)).await.unwrap(),
                        Some(format!("value{}", key_id).into_bytes())
                    );
                }
            }));
        }
        for task in tasks {
            task.await;
        }

        // Open from disk again and check persistent data
        drop(store);
        let store = Arc::new(KvStore::open(temp_dir.path()).await?);
        let mut tasks = Vec::with_capacity(100);
        for id in 0..100 {
            let store = Arc::clone(&store);
            tasks.push(task::spawn(async move {
                for i in 0..100 {
                    let key_id = (i + id) % 100;
                    assert_eq!(
                        store.get(format!("key{}", key_id)).await.unwrap(),
                        Some(format!("value{}", key_id).into_bytes())
                    );
                }
            }));
        }
        for task in tasks {
            task.await;
        }
        Ok(())
    })
}
