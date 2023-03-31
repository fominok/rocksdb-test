use std::{thread, time::Duration};

use rocksdb::{MultiThreaded, OptimisticTransactionDB};
use tempfile::TempDir;

const NICE: u32 = 1337;

fn main() {
    let tempdir = TempDir::new().expect("cannot create a tempdir");
    let db = OptimisticTransactionDB::<MultiThreaded>::open_default(tempdir.path())
        .expect("cannot open RocskDB");

    (0..NICE).for_each(|i| {
        db.put(format!("key {i}"), format!("value {i}"))
            .expect("cannot put into RocksDB")
    });

    println!("Init data inserted");

    crossbeam::scope(|s| {
        // A write thread (one and only)
        s.spawn(|_| {
            let tx = db.transaction();
            (0..NICE).for_each(|i| {
                tx.put(format!("key {i}"), format!("value {}", i + NICE))
                    .expect("cannot put into RocksDB")
            });

            // Artificially slow thing thread down to keep transaction longer
            println!("Finished doing writes, but not yet commited");
            thread::sleep(Duration::from_secs(5));

            tx.commit().expect("cannot commit transaction");

            println!("Transaction commited");
        });

        // Some read threads, also in transactions
        (0..20).for_each(|thread_index| {
            let db = &db;
            s.spawn(move |_| {
                (0..NICE).for_each(|i| {
                    let tx = db.transaction();
                    let value = tx.get(format!("key {i}")).expect("cannot get value");
                    assert_eq!(value, Some(format!("value {i}").into_bytes()));
                });

                println!("Read {thread_index} done");
            });
        });
    })
    .expect("some thread(s) finished with errors");

    // Check that data was actually commited
    println!("Validating commited data");

    (0..NICE).for_each(|i| {
        let value = db.get(format!("key {i}")).expect("cannot get value");
        assert_eq!(value, Some(format!("value {}", i + NICE).into_bytes()));
    });

    println!("All done");
}
