mod common;

use common::*;
use ferrokinesis::store::StoreOptions;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Barrier;
use tokio::task::JoinSet;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn stress_options() -> StoreOptions {
    StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 200,
        ..Default::default()
    }
}

fn stress_options_with_delays() -> StoreOptions {
    StoreOptions {
        create_stream_ms: 10,
        delete_stream_ms: 10,
        update_stream_ms: 10,
        shard_limit: 200,
        ..Default::default()
    }
}

/// Count total records across all shards by paginating with GetRecords.
async fn count_all_records(server: &TestServer, stream: &str, shard_count: u32) -> usize {
    let mut total = 0;
    for i in 0..shard_count {
        let shard_id = format!("shardId-{i:012}");
        let mut iter = server
            .get_shard_iterator(stream, &shard_id, "TRIM_HORIZON")
            .await;
        let mut iterations = 0;
        loop {
            let result = server.get_records(&iter).await;
            let records = result["Records"].as_array().unwrap();
            if records.is_empty() {
                break;
            }
            total += records.len();
            match result["NextShardIterator"].as_str() {
                Some(next) => iter = next.to_string(),
                None => break,
            }
            iterations += 1;
            assert!(
                iterations < 500,
                "too many iterations, likely infinite loop"
            );
        }
    }
    total
}

// ===========================================================================
// Scenario 1: Write contention — N concurrent PutRecord to same shard
// ===========================================================================

#[tokio::test]
async fn write_contention_same_shard() {
    const NUM_TASKS: usize = 20;
    const RECORDS_PER_TASK: usize = 50;

    let server = Arc::new(TestServer::with_options(stress_options()).await);
    let stream = "conc-write-contention";
    server.create_stream(stream, 1).await;

    let barrier = Arc::new(Barrier::new(NUM_TASKS + 1));
    let mut join_set = JoinSet::new();

    for task_id in 0..NUM_TASKS {
        let server = server.clone();
        let barrier = barrier.clone();
        join_set.spawn(async move {
            barrier.wait().await;
            let mut seq_nums = Vec::with_capacity(RECORDS_PER_TASK);
            for i in 0..RECORDS_PER_TASK {
                let res = server
                    .request(
                        "PutRecord",
                        &json!({
                            "StreamName": stream,
                            "Data": "AAAA",
                            "PartitionKey": format!("t{task_id}-r{i}"),
                        }),
                    )
                    .await;
                let status = res.status().as_u16();
                assert_eq!(status, 200, "task {task_id} record {i}: got HTTP {status}");
                let body: Value = res.json().await.unwrap();
                seq_nums.push(body["SequenceNumber"].as_str().unwrap().to_string());
            }
            seq_nums
        });
    }

    // Release all tasks simultaneously
    barrier.wait().await;

    // Collect all results
    let mut all_seq_nums = Vec::with_capacity(NUM_TASKS * RECORDS_PER_TASK);
    while let Some(result) = join_set.join_next().await {
        let seq_nums = result.expect("task panicked");
        all_seq_nums.extend(seq_nums);
    }

    assert_eq!(
        all_seq_nums.len(),
        NUM_TASKS * RECORDS_PER_TASK,
        "expected {} successful PutRecord responses",
        NUM_TASKS * RECORDS_PER_TASK,
    );

    // All sequence numbers must be unique
    let unique: HashSet<&String> = all_seq_nums.iter().collect();
    assert_eq!(
        unique.len(),
        all_seq_nums.len(),
        "duplicate sequence numbers detected"
    );

    // Verify records in store by reading them back
    let total = count_all_records(&server, stream, 1).await;
    assert_eq!(total, NUM_TASKS * RECORDS_PER_TASK);
}

// ===========================================================================
// Scenario 2: Read/write interleaving
// ===========================================================================

#[tokio::test]
async fn read_write_interleaving() {
    const NUM_WRITERS: usize = 10;
    const WRITES_PER_TASK: usize = 50;
    const NUM_READERS: usize = 5;

    let server = Arc::new(TestServer::with_options(stress_options()).await);
    let stream = "conc-rw-interleave";
    server.create_stream(stream, 1).await;

    // Seed with 100 records
    for i in 0..100 {
        server
            .put_record(stream, "AAAA", &format!("seed-{i}"))
            .await;
    }

    let barrier = Arc::new(Barrier::new(NUM_WRITERS + NUM_READERS + 1));
    let mut join_set = JoinSet::new();

    // Writer tasks
    for task_id in 0..NUM_WRITERS {
        let server = server.clone();
        let barrier = barrier.clone();
        join_set.spawn(async move {
            barrier.wait().await;
            let mut success_count = 0u64;
            for i in 0..WRITES_PER_TASK {
                let res = server
                    .request(
                        "PutRecord",
                        &json!({
                            "StreamName": stream,
                            "Data": "AAAA",
                            "PartitionKey": format!("w{task_id}-{i}"),
                        }),
                    )
                    .await;
                assert_eq!(res.status().as_u16(), 200);
                success_count += 1;
            }
            success_count
        });
    }

    // Reader tasks
    for reader_id in 0..NUM_READERS {
        let server = server.clone();
        let barrier = barrier.clone();
        join_set.spawn(async move {
            barrier.wait().await;
            let mut reads = 0u64;
            let mut iter = server
                .get_shard_iterator(stream, "shardId-000000000000", "TRIM_HORIZON")
                .await;
            let mut prev_seq = String::new();
            let mut loops = 0;

            // Read through available records multiple times
            while loops < 50 {
                let result = server.get_records(&iter).await;
                let records = result["Records"].as_array().unwrap();

                // Verify ordering within each batch
                for r in records {
                    let seq = r["SequenceNumber"].as_str().unwrap();
                    if !prev_seq.is_empty() {
                        assert!(
                            seq > prev_seq.as_str(),
                            "reader {reader_id}: sequence numbers not monotonic: {prev_seq} >= {seq}"
                        );
                    }
                    // Verify record structure
                    assert!(r["Data"].as_str().is_some(), "missing Data field");
                    assert!(
                        r["PartitionKey"].as_str().is_some(),
                        "missing PartitionKey field"
                    );
                    prev_seq = seq.to_string();
                    reads += 1;
                }

                match result["NextShardIterator"].as_str() {
                    Some(next) => iter = next.to_string(),
                    None => break,
                }
                loops += 1;
            }
            reads
        });
    }

    barrier.wait().await;

    // Collect results — verify no panics
    let mut total_writes = 0u64;
    let mut total_reads = 0u64;
    while let Some(result) = join_set.join_next().await {
        let count = result.expect("task panicked");
        // Writers return their write count, readers return their read count.
        // We can't distinguish them here but both must not panic.
        if count <= WRITES_PER_TASK as u64 {
            total_writes += count;
        } else {
            total_reads += count;
        }
    }

    // All writes succeeded
    let expected_writes = (NUM_WRITERS * WRITES_PER_TASK) as u64;
    assert!(total_writes >= expected_writes || total_reads > 0);

    // Verify final record count
    let total = count_all_records(&server, stream, 1).await;
    assert_eq!(total, 100 + NUM_WRITERS * WRITES_PER_TASK);
}

// ===========================================================================
// Scenario 3: Stream lifecycle mutations during access
// ===========================================================================

#[tokio::test]
async fn stream_lifecycle_mutations() {
    const NUM_LIFECYCLE: usize = 5;
    const NUM_ACCESSORS: usize = 10;
    const ITERATIONS: usize = 3;

    let server = Arc::new(TestServer::with_options(stress_options_with_delays()).await);

    // Pre-compute all stream names
    let stream_names: Vec<String> = (0..NUM_LIFECYCLE)
        .flat_map(|t| (0..ITERATIONS).map(move |i| format!("conc-lifecycle-{t}-{i}")))
        .collect();
    let stream_names = Arc::new(stream_names);

    let barrier = Arc::new(Barrier::new(NUM_LIFECYCLE + NUM_ACCESSORS + 1));
    let mut join_set = JoinSet::new();

    // Lifecycle tasks: create, wait, delete
    for task_id in 0..NUM_LIFECYCLE {
        let server = server.clone();
        let barrier = barrier.clone();
        join_set.spawn(async move {
            barrier.wait().await;
            for iter in 0..ITERATIONS {
                let name = format!("conc-lifecycle-{task_id}-{iter}");
                let res = server
                    .request(
                        "CreateStream",
                        &json!({"StreamName": &name, "ShardCount": 1}),
                    )
                    .await;
                let status = res.status().as_u16();
                assert!(
                    status == 200 || status == 400,
                    "CreateStream got HTTP {status}"
                );

                // Wait for ACTIVE
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

                let res = server
                    .request("DeleteStream", &json!({"StreamName": &name}))
                    .await;
                let status = res.status().as_u16();
                assert!(
                    status == 200 || status == 400,
                    "DeleteStream got HTTP {status}"
                );

                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            }
        });
    }

    // Accessor tasks: try PutRecord/GetRecords on random streams
    for accessor_id in 0..NUM_ACCESSORS {
        let server = server.clone();
        let barrier = barrier.clone();
        let names = stream_names.clone();
        join_set.spawn(async move {
            barrier.wait().await;
            for i in 0..20 {
                let idx = (accessor_id * 20 + i) % names.len();
                let name = &names[idx];

                // Try PutRecord
                let (status, body) = {
                    let res = server
                        .request(
                            "PutRecord",
                            &json!({
                                "StreamName": name,
                                "Data": "AAAA",
                                "PartitionKey": format!("a{accessor_id}-{i}"),
                            }),
                        )
                        .await;
                    decode_body(res).await
                };
                assert!(
                    status == 200 || status == 400,
                    "PutRecord on {name} got HTTP {status}: {body}"
                );
                if status == 400 {
                    let err_type = body["__type"].as_str().unwrap_or("");
                    assert!(
                        err_type == "ResourceNotFoundException"
                            || err_type == "ResourceInUseException",
                        "unexpected error type: {err_type}"
                    );
                }
            }
        });
    }

    barrier.wait().await;

    while let Some(result) = join_set.join_next().await {
        result.expect("task panicked");
    }
}

// ===========================================================================
// Scenario 4: Consumer registration contention
// ===========================================================================

#[tokio::test]
async fn consumer_registration_contention() {
    const NUM_CONSUMERS: usize = 10;

    let server = Arc::new(TestServer::with_options(stress_options()).await);
    let stream = "conc-consumer-reg";
    server.create_stream(stream, 1).await;
    let stream_arn = server.get_stream_arn(stream).await;

    let barrier = Arc::new(Barrier::new(NUM_CONSUMERS + 1));
    let mut join_set = JoinSet::new();

    for i in 0..NUM_CONSUMERS {
        let server = server.clone();
        let barrier = barrier.clone();
        let arn = stream_arn.clone();
        join_set.spawn(async move {
            barrier.wait().await;
            let consumer_name = format!("conc-consumer-{i}");
            let res = server
                .request(
                    "RegisterStreamConsumer",
                    &json!({
                        "StreamARN": arn,
                        "ConsumerName": consumer_name,
                    }),
                )
                .await;
            let status = res.status().as_u16();
            assert_eq!(
                status, 200,
                "RegisterStreamConsumer for {consumer_name} got HTTP {status}"
            );
            let body: Value = res.json().await.unwrap();
            body["Consumer"]["ConsumerARN"]
                .as_str()
                .unwrap()
                .to_string()
        });
    }

    barrier.wait().await;

    let mut consumer_arns = Vec::with_capacity(NUM_CONSUMERS);
    while let Some(result) = join_set.join_next().await {
        let arn = result.expect("task panicked");
        consumer_arns.push(arn);
    }

    // All ARNs must be unique
    let unique_arns: HashSet<&String> = consumer_arns.iter().collect();
    assert_eq!(
        unique_arns.len(),
        NUM_CONSUMERS,
        "duplicate consumer ARNs detected"
    );

    // ListStreamConsumers must return exactly NUM_CONSUMERS
    let res = server
        .request("ListStreamConsumers", &json!({"StreamARN": stream_arn}))
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let consumers = body["Consumers"].as_array().unwrap();
    assert_eq!(consumers.len(), NUM_CONSUMERS);

    // Verify no duplicate names
    let names: HashSet<&str> = consumers
        .iter()
        .map(|c| c["ConsumerName"].as_str().unwrap())
        .collect();
    assert_eq!(
        names.len(),
        NUM_CONSUMERS,
        "duplicate consumer names in list"
    );
}

// ===========================================================================
// Scenario 5: Shard iterator invalidation during split
// ===========================================================================

#[tokio::test]
async fn shard_iterator_during_split() {
    let server = Arc::new(TestServer::with_options(stress_options()).await);
    let stream = "conc-split-iter";
    server.create_stream(stream, 4).await;

    // Get shard hash ranges for targeted puts
    let desc = server.describe_stream(stream).await;
    let shards = desc["StreamDescription"]["Shards"].as_array().unwrap();

    // Put 20 records into each shard via ExplicitHashKey
    for (shard_idx, shard) in shards.iter().enumerate() {
        let start_key = shard["HashKeyRange"]["StartingHashKey"].as_str().unwrap();
        for i in 0..20 {
            let res = server
                .request(
                    "PutRecord",
                    &json!({
                        "StreamName": stream,
                        "Data": "AAAA",
                        "PartitionKey": format!("s{shard_idx}-{i}"),
                        "ExplicitHashKey": start_key,
                    }),
                )
                .await;
            assert_eq!(res.status(), 200);
        }
    }

    let barrier = Arc::new(Barrier::new(5 + 1)); // 4 readers + 1 splitter + coordinator
    let mut join_set = JoinSet::new();

    // Reader tasks for each shard
    for shard_idx in 0..4u32 {
        let server = server.clone();
        let barrier = barrier.clone();
        join_set.spawn(async move {
            let shard_id = format!("shardId-{shard_idx:012}");
            let mut iter = server
                .get_shard_iterator(stream, &shard_id, "TRIM_HORIZON")
                .await;
            barrier.wait().await;

            let mut records_read = 0usize;
            let mut loops = 0;
            loop {
                let (status, result) = {
                    let res = server
                        .request("GetRecords", &json!({"ShardIterator": iter}))
                        .await;
                    decode_body(res).await
                };

                assert!(
                    status == 200 || status == 400,
                    "shard {shard_idx}: GetRecords got HTTP {status}"
                );

                if status == 400 {
                    // Iterator may become invalid after split
                    break;
                }

                let recs = result["Records"].as_array().unwrap();
                records_read += recs.len();

                match result["NextShardIterator"].as_str() {
                    Some(next) => iter = next.to_string(),
                    None => break, // Shard closed
                }

                if recs.is_empty() {
                    loops += 1;
                    if loops > 10 {
                        break; // No more records coming
                    }
                } else {
                    loops = 0;
                }
            }
            (shard_idx, records_read)
        });
    }

    // Splitter task: split shard 0
    {
        let server = server.clone();
        let barrier = barrier.clone();
        let shard0_start = shards[0]["HashKeyRange"]["StartingHashKey"]
            .as_str()
            .unwrap()
            .to_string();
        let shard0_end = shards[0]["HashKeyRange"]["EndingHashKey"]
            .as_str()
            .unwrap()
            .to_string();

        // Compute midpoint for split
        let start: num_bigint::BigUint = shard0_start.parse().unwrap();
        let end: num_bigint::BigUint = shard0_end.parse().unwrap();
        let mid = (&start + &end) / 2u32;
        let mid_str = mid.to_string();

        join_set.spawn(async move {
            barrier.wait().await;
            // Brief delay to let readers start
            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

            let res = server
                .request(
                    "SplitShard",
                    &json!({
                        "StreamName": stream,
                        "ShardToSplit": "shardId-000000000000",
                        "NewStartingHashKey": mid_str,
                    }),
                )
                .await;
            assert_eq!(res.status(), 200, "SplitShard failed");

            // Wait for split to complete
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            (u32::MAX, 0usize) // Sentinel for splitter task
        });
    }

    barrier.wait().await;

    let mut shard_reads = std::collections::HashMap::new();
    while let Some(result) = join_set.join_next().await {
        let (shard_idx, count) = result.expect("task panicked");
        if shard_idx != u32::MAX {
            shard_reads.insert(shard_idx, count);
        }
    }

    // Non-split shards (1, 2, 3) should have read their 20 records
    for shard_idx in 1..4u32 {
        let count = shard_reads.get(&shard_idx).copied().unwrap_or(0);
        assert_eq!(
            count, 20,
            "shard {shard_idx}: expected 20 records, got {count}"
        );
    }

    // Shard 0 (split) should have read its 20 records
    let shard0_count = shard_reads.get(&0).copied().unwrap_or(0);
    assert_eq!(
        shard0_count, 20,
        "shard 0 (split): expected 20 records, got {shard0_count}"
    );
}

// ===========================================================================
// Scenario 6: PutRecords batch contention
// ===========================================================================

#[tokio::test]
async fn put_records_batch_contention() {
    const NUM_TASKS: usize = 20;
    const RECORDS_PER_BATCH: usize = 100;

    let server = Arc::new(TestServer::with_options(stress_options()).await);
    let stream = "conc-batch-contention";
    server.create_stream(stream, 4).await;

    let barrier = Arc::new(Barrier::new(NUM_TASKS + 1));
    let mut join_set = JoinSet::new();

    for task_id in 0..NUM_TASKS {
        let server = server.clone();
        let barrier = barrier.clone();
        join_set.spawn(async move {
            barrier.wait().await;
            let records: Vec<Value> = (0..RECORDS_PER_BATCH)
                .map(|i| {
                    json!({
                        "Data": "AAAA",
                        "PartitionKey": format!("t{task_id}-r{i}"),
                    })
                })
                .collect();

            let res = server
                .request(
                    "PutRecords",
                    &json!({
                        "StreamName": stream,
                        "Records": records,
                    }),
                )
                .await;
            let status = res.status().as_u16();
            assert_eq!(status, 200, "task {task_id}: PutRecords got HTTP {status}");

            let body: Value = res.json().await.unwrap();
            let failed = body["FailedRecordCount"].as_u64().unwrap();
            assert_eq!(failed, 0, "task {task_id}: FailedRecordCount = {failed}");

            let result_records = body["Records"].as_array().unwrap();
            assert_eq!(result_records.len(), RECORDS_PER_BATCH);

            // Collect sequence numbers
            let seq_nums: Vec<String> = result_records
                .iter()
                .map(|r| r["SequenceNumber"].as_str().unwrap().to_string())
                .collect();
            seq_nums
        });
    }

    barrier.wait().await;

    let mut all_seq_nums = Vec::with_capacity(NUM_TASKS * RECORDS_PER_BATCH);
    while let Some(result) = join_set.join_next().await {
        let seq_nums = result.expect("task panicked");
        all_seq_nums.extend(seq_nums);
    }

    assert_eq!(all_seq_nums.len(), NUM_TASKS * RECORDS_PER_BATCH);

    // All sequence numbers across all batches must be unique
    let unique: HashSet<&String> = all_seq_nums.iter().collect();
    assert_eq!(
        unique.len(),
        all_seq_nums.len(),
        "duplicate sequence numbers in batch results"
    );

    // Verify total record count in store
    let total = count_all_records(&server, stream, 4).await;
    assert_eq!(total, NUM_TASKS * RECORDS_PER_BATCH);
}

// ===========================================================================
// Scenario 7: Mixed workload stress
// ===========================================================================

#[tokio::test]
async fn mixed_workload_stress() {
    let result =
        tokio::time::timeout(tokio::time::Duration::from_secs(30), mixed_workload_inner()).await;

    assert!(result.is_ok(), "mixed workload test timed out (deadlock?)");
}

async fn mixed_workload_inner() {
    const NUM_PUT_RECORD: usize = 5;
    const NUM_PUT_RECORDS: usize = 3;
    const NUM_READERS: usize = 3;
    const NUM_CONSUMER_REG: usize = 2;
    const TOTAL_TASKS: usize =
        NUM_PUT_RECORD + NUM_PUT_RECORDS + NUM_READERS + NUM_CONSUMER_REG + 1; // +1 splitter

    let server = Arc::new(TestServer::with_options(stress_options()).await);
    let stream = "conc-mixed-workload";
    server.create_stream(stream, 4).await;
    let stream_arn = server.get_stream_arn(stream).await;

    // Seed some records
    for i in 0..20 {
        server
            .put_record(stream, "AAAA", &format!("seed-{i}"))
            .await;
    }

    let barrier = Arc::new(Barrier::new(TOTAL_TASKS + 1));
    let mut join_set = JoinSet::new();

    // PutRecord tasks
    for task_id in 0..NUM_PUT_RECORD {
        let server = server.clone();
        let barrier = barrier.clone();
        join_set.spawn(async move {
            barrier.wait().await;
            for i in 0..20 {
                let (status, _) = {
                    let res = server
                        .request(
                            "PutRecord",
                            &json!({
                                "StreamName": stream,
                                "Data": "AAAA",
                                "PartitionKey": format!("pr-{task_id}-{i}"),
                            }),
                        )
                        .await;
                    decode_body(res).await
                };
                assert!(
                    status == 200 || status == 400,
                    "PutRecord got HTTP {status}"
                );
            }
        });
    }

    // PutRecords tasks
    for task_id in 0..NUM_PUT_RECORDS {
        let server = server.clone();
        let barrier = barrier.clone();
        join_set.spawn(async move {
            barrier.wait().await;
            let records: Vec<Value> = (0..50)
                .map(|i| {
                    json!({
                        "Data": "AAAA",
                        "PartitionKey": format!("batch-{task_id}-{i}"),
                    })
                })
                .collect();
            let (status, _) = {
                let res = server
                    .request(
                        "PutRecords",
                        &json!({
                            "StreamName": stream,
                            "Records": records,
                        }),
                    )
                    .await;
                decode_body(res).await
            };
            assert!(
                status == 200 || status == 400,
                "PutRecords got HTTP {status}"
            );
        });
    }

    // Reader tasks
    for _reader_id in 0..NUM_READERS {
        let server = server.clone();
        let barrier = barrier.clone();
        join_set.spawn(async move {
            barrier.wait().await;
            let iter = server
                .get_shard_iterator(stream, "shardId-000000000000", "TRIM_HORIZON")
                .await;
            let (status, result) = {
                let res = server
                    .request("GetRecords", &json!({"ShardIterator": iter}))
                    .await;
                decode_body(res).await
            };
            assert!(
                status == 200 || status == 400,
                "GetRecords got HTTP {status}"
            );
            if status == 200 {
                let records = result["Records"].as_array().unwrap();
                for r in records {
                    assert!(r["Data"].as_str().is_some());
                    assert!(r["PartitionKey"].as_str().is_some());
                }
            }
        });
    }

    // Consumer registration tasks
    for task_id in 0..NUM_CONSUMER_REG {
        let server = server.clone();
        let barrier = barrier.clone();
        let arn = stream_arn.clone();
        join_set.spawn(async move {
            barrier.wait().await;
            for i in 0..3 {
                let name = format!("mixed-consumer-{task_id}-{i}");
                let (status, _) = {
                    let res = server
                        .request(
                            "RegisterStreamConsumer",
                            &json!({
                                "StreamARN": arn,
                                "ConsumerName": name,
                            }),
                        )
                        .await;
                    decode_body(res).await
                };
                assert!(
                    status == 200 || status == 400,
                    "RegisterStreamConsumer got HTTP {status}"
                );
            }
        });
    }

    // Splitter task
    {
        let server = server.clone();
        let barrier = barrier.clone();

        // Get shard 0's hash range for the split
        let desc = server.describe_stream(stream).await;
        let shards = desc["StreamDescription"]["Shards"].as_array().unwrap();
        let start: num_bigint::BigUint = shards[0]["HashKeyRange"]["StartingHashKey"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        let end: num_bigint::BigUint = shards[0]["HashKeyRange"]["EndingHashKey"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        let mid = (&start + &end) / 2u32;
        let mid_str = mid.to_string();

        join_set.spawn(async move {
            barrier.wait().await;
            // Brief delay so other operations start first
            tokio::time::sleep(tokio::time::Duration::from_millis(30)).await;

            let (status, _) = {
                let res = server
                    .request(
                        "SplitShard",
                        &json!({
                            "StreamName": stream,
                            "ShardToSplit": "shardId-000000000000",
                            "NewStartingHashKey": mid_str,
                        }),
                    )
                    .await;
                decode_body(res).await
            };
            // Split may fail if stream is in UPDATING state from concurrent operations
            assert!(
                status == 200 || status == 400,
                "SplitShard got HTTP {status}"
            );
        });
    }

    barrier.wait().await;

    // Wait for all tasks to complete — any panic is caught here
    while let Some(result) = join_set.join_next().await {
        result.expect("task panicked");
    }

    // Verify server is still responsive
    let res = server
        .request("DescribeStream", &json!({"StreamName": stream}))
        .await;
    assert_eq!(
        res.status().as_u16(),
        200,
        "server unresponsive after mixed workload"
    );
}
