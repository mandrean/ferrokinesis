use super::{FieldDef, FieldType};

fn stream_name_field() -> FieldDef {
    FieldDef::new(FieldType::String)
        .not_null()
        .regex("[a-zA-Z0-9_.-]+")
        .len_gte(1)
        .len_lte(128)
}

/// StreamName without not_null — for actions that accept StreamARN as alternative.
fn stream_name_optional_field() -> FieldDef {
    FieldDef::new(FieldType::String)
        .regex("[a-zA-Z0-9_.-]+")
        .len_gte(1)
        .len_lte(128)
}

pub fn create_stream() -> Vec<(&'static str, FieldDef)> {
    vec![
        (
            "ShardCount",
            FieldDef::new(FieldType::Integer)
                .not_null()
                .gte(1.0)
                .lte(100000.0),
        ),
        ("StreamName", stream_name_field()),
    ]
}

pub fn delete_stream() -> Vec<(&'static str, FieldDef)> {
    vec![("StreamName", stream_name_field())]
}

pub fn describe_stream() -> Vec<(&'static str, FieldDef)> {
    vec![
        (
            "Limit",
            FieldDef::new(FieldType::Integer).gte(1.0).lte(10000.0),
        ),
        (
            "ExclusiveStartShardId",
            FieldDef::new(FieldType::String)
                .regex("[a-zA-Z0-9_.-]+")
                .len_gte(1)
                .len_lte(128),
        ),
        ("StreamName", stream_name_field()),
    ]
}

pub fn describe_stream_summary() -> Vec<(&'static str, FieldDef)> {
    vec![("StreamName", stream_name_field())]
}

pub fn list_streams() -> Vec<(&'static str, FieldDef)> {
    vec![
        (
            "Limit",
            FieldDef::new(FieldType::Integer).gte(1.0).lte(10000.0),
        ),
        (
            "ExclusiveStartStreamName",
            FieldDef::new(FieldType::String)
                .regex("[a-zA-Z0-9_.-]+")
                .len_gte(1)
                .len_lte(128),
        ),
    ]
}

pub fn list_shards() -> Vec<(&'static str, FieldDef)> {
    vec![
        (
            "ExclusiveStartShardId",
            FieldDef::new(FieldType::String)
                .regex("[a-zA-Z0-9_.-]+")
                .len_gte(1)
                .len_lte(128),
        ),
        (
            "MaxResults",
            FieldDef::new(FieldType::Integer).gte(1.0).lte(10000.0),
        ),
        ("NextToken", FieldDef::new(FieldType::String).len_gte(1)),
        (
            "StreamCreationTimestamp",
            FieldDef::new(FieldType::Timestamp),
        ),
        (
            "StreamName",
            FieldDef::new(FieldType::String)
                .regex("[a-zA-Z0-9_.-]+")
                .len_gte(1)
                .len_lte(128),
        ),
    ]
}

pub fn put_record() -> Vec<(&'static str, FieldDef)> {
    vec![
        (
            "SequenceNumberForOrdering",
            FieldDef::new(FieldType::String).regex("0|([1-9]\\d{0,128})"),
        ),
        (
            "PartitionKey",
            FieldDef::new(FieldType::String)
                .not_null()
                .len_gte(1)
                .len_lte(256),
        ),
        (
            "ExplicitHashKey",
            FieldDef::new(FieldType::String).regex("0|([1-9]\\d{0,38})"),
        ),
        (
            "Data",
            FieldDef::new(FieldType::Blob).not_null().len_lte(1048576),
        ),
        ("StreamARN", stream_arn_field()),
        ("StreamName", stream_name_optional_field()),
    ]
}

pub fn put_records() -> Vec<(&'static str, FieldDef)> {
    vec![
        (
            "Records",
            FieldDef::new(FieldType::List {
                children: Box::new(FieldDef::new(FieldType::Structure {
                    children: vec![
                        (
                            "PartitionKey".to_string(),
                            FieldDef::new(FieldType::String)
                                .not_null()
                                .len_gte(1)
                                .len_lte(256),
                        ),
                        (
                            "ExplicitHashKey".to_string(),
                            FieldDef::new(FieldType::String).regex("0|([1-9]\\d{0,38})"),
                        ),
                        (
                            "Data".to_string(),
                            FieldDef::new(FieldType::Blob).not_null().len_lte(1048576),
                        ),
                    ],
                })),
            })
            .not_null()
            .len_gte(1)
            .len_lte(500)
            .member_str("com.amazonaws.kinesis.v20131202.PutRecordsRequestEntry@c965e310"),
        ),
        ("StreamARN", stream_arn_field()),
        ("StreamName", stream_name_optional_field()),
    ]
}

pub fn get_records() -> Vec<(&'static str, FieldDef)> {
    vec![
        (
            "Limit",
            FieldDef::new(FieldType::Integer).gte(1.0).lte(10000.0),
        ),
        (
            "ShardIterator",
            FieldDef::new(FieldType::String)
                .not_null()
                .len_gte(1)
                .len_lte(512),
        ),
    ]
}

pub fn get_shard_iterator() -> Vec<(&'static str, FieldDef)> {
    vec![
        (
            "ShardId",
            FieldDef::new(FieldType::String)
                .not_null()
                .regex("[a-zA-Z0-9_.-]+")
                .len_gte(1)
                .len_lte(128),
        ),
        (
            "ShardIteratorType",
            FieldDef::new(FieldType::String)
                .not_null()
                .enum_values(vec![
                    "AFTER_SEQUENCE_NUMBER",
                    "LATEST",
                    "AT_TIMESTAMP",
                    "AT_SEQUENCE_NUMBER",
                    "TRIM_HORIZON",
                ]),
        ),
        (
            "StartingSequenceNumber",
            FieldDef::new(FieldType::String).regex("0|([1-9]\\d{0,128})"),
        ),
        ("StreamName", stream_name_field()),
        ("Timestamp", FieldDef::new(FieldType::Timestamp)),
    ]
}

pub fn add_tags_to_stream() -> Vec<(&'static str, FieldDef)> {
    vec![
        (
            "Tags",
            FieldDef::new(FieldType::Map {
                children: Box::new(FieldDef::new(FieldType::String)),
            })
            .not_null()
            .len_gte(1)
            .len_lte(10)
            .child_key_lengths(1, 128)
            .child_value_lengths(0, 256),
        ),
        ("StreamName", stream_name_field()),
    ]
}

pub fn remove_tags_from_stream() -> Vec<(&'static str, FieldDef)> {
    vec![
        (
            "TagKeys",
            FieldDef::new(FieldType::List {
                children: Box::new(FieldDef::new(FieldType::String)),
            })
            .not_null()
            .len_gte(1)
            .len_lte(10)
            .child_lengths(1, 128),
        ),
        ("StreamName", stream_name_field()),
    ]
}

pub fn list_tags_for_stream() -> Vec<(&'static str, FieldDef)> {
    vec![
        (
            "Limit",
            FieldDef::new(FieldType::Integer).gte(1.0).lte(10.0),
        ),
        (
            "ExclusiveStartTagKey",
            FieldDef::new(FieldType::String).len_gte(1).len_lte(128),
        ),
        ("StreamName", stream_name_field()),
    ]
}

pub fn merge_shards() -> Vec<(&'static str, FieldDef)> {
    vec![
        (
            "ShardToMerge",
            FieldDef::new(FieldType::String)
                .not_null()
                .regex("[a-zA-Z0-9_.-]+")
                .len_gte(1)
                .len_lte(128),
        ),
        (
            "AdjacentShardToMerge",
            FieldDef::new(FieldType::String)
                .not_null()
                .regex("[a-zA-Z0-9_.-]+")
                .len_gte(1)
                .len_lte(128),
        ),
        ("StreamName", stream_name_field()),
    ]
}

pub fn split_shard() -> Vec<(&'static str, FieldDef)> {
    vec![
        (
            "NewStartingHashKey",
            FieldDef::new(FieldType::String)
                .not_null()
                .regex("0|([1-9]\\d{0,38})"),
        ),
        ("StreamName", stream_name_field()),
        (
            "ShardToSplit",
            FieldDef::new(FieldType::String)
                .not_null()
                .regex("[a-zA-Z0-9_.-]+")
                .len_gte(1)
                .len_lte(128),
        ),
    ]
}

pub fn increase_stream_retention_period() -> Vec<(&'static str, FieldDef)> {
    vec![
        (
            "RetentionPeriodHours",
            FieldDef::new(FieldType::Integer)
                .not_null()
                .gte(1.0)
                .lte(168.0),
        ),
        ("StreamName", stream_name_field()),
    ]
}

pub fn decrease_stream_retention_period() -> Vec<(&'static str, FieldDef)> {
    vec![
        (
            "RetentionPeriodHours",
            FieldDef::new(FieldType::Integer)
                .not_null()
                .gte(1.0)
                .lte(168.0),
        ),
        ("StreamName", stream_name_field()),
    ]
}

fn stream_arn_field() -> FieldDef {
    FieldDef::new(FieldType::String).len_gte(1).len_lte(2048)
}

fn consumer_name_field() -> FieldDef {
    FieldDef::new(FieldType::String)
        .regex("[a-zA-Z0-9_.-]+")
        .len_gte(1)
        .len_lte(128)
}

pub fn enable_enhanced_monitoring() -> Vec<(&'static str, FieldDef)> {
    vec![
        (
            "ShardLevelMetrics",
            FieldDef::new(FieldType::List {
                children: Box::new(FieldDef::new(FieldType::String)),
            })
            .not_null()
            .len_gte(1)
            .len_lte(7),
        ),
        ("StreamName", stream_name_field()),
        ("StreamARN", stream_arn_field()),
    ]
}

pub fn disable_enhanced_monitoring() -> Vec<(&'static str, FieldDef)> {
    enable_enhanced_monitoring()
}

pub fn start_stream_encryption() -> Vec<(&'static str, FieldDef)> {
    vec![
        (
            "EncryptionType",
            FieldDef::new(FieldType::String)
                .not_null()
                .enum_values(vec!["KMS"]),
        ),
        (
            "KeyId",
            FieldDef::new(FieldType::String)
                .not_null()
                .len_gte(1)
                .len_lte(2048),
        ),
        ("StreamName", stream_name_field()),
        ("StreamARN", stream_arn_field()),
    ]
}

pub fn stop_stream_encryption() -> Vec<(&'static str, FieldDef)> {
    vec![
        (
            "EncryptionType",
            FieldDef::new(FieldType::String)
                .not_null()
                .enum_values(vec!["KMS"]),
        ),
        (
            "KeyId",
            FieldDef::new(FieldType::String)
                .not_null()
                .len_gte(1)
                .len_lte(2048),
        ),
        ("StreamName", stream_name_field()),
        ("StreamARN", stream_arn_field()),
    ]
}

pub fn register_stream_consumer() -> Vec<(&'static str, FieldDef)> {
    vec![
        ("ConsumerName", consumer_name_field().not_null()),
        ("StreamARN", stream_arn_field().not_null()),
    ]
}

pub fn deregister_stream_consumer() -> Vec<(&'static str, FieldDef)> {
    vec![
        (
            "ConsumerARN",
            FieldDef::new(FieldType::String).len_gte(1).len_lte(2048),
        ),
        ("ConsumerName", consumer_name_field()),
        ("StreamARN", stream_arn_field()),
    ]
}

pub fn describe_stream_consumer() -> Vec<(&'static str, FieldDef)> {
    deregister_stream_consumer()
}

pub fn list_stream_consumers() -> Vec<(&'static str, FieldDef)> {
    vec![
        (
            "MaxResults",
            FieldDef::new(FieldType::Integer).gte(1.0).lte(10000.0),
        ),
        (
            "NextToken",
            FieldDef::new(FieldType::String).len_gte(1).len_lte(1048576),
        ),
        ("StreamARN", stream_arn_field().not_null()),
        (
            "StreamCreationTimestamp",
            FieldDef::new(FieldType::Timestamp),
        ),
    ]
}

pub fn update_shard_count() -> Vec<(&'static str, FieldDef)> {
    vec![
        (
            "ScalingType",
            FieldDef::new(FieldType::String)
                .not_null()
                .enum_values(vec!["UNIFORM_SCALING"]),
        ),
        ("StreamARN", stream_arn_field()),
        ("StreamName", stream_name_field()),
        (
            "TargetShardCount",
            FieldDef::new(FieldType::Integer)
                .not_null()
                .gte(1.0)
                .lte(100000.0),
        ),
    ]
}

pub fn update_stream_mode() -> Vec<(&'static str, FieldDef)> {
    vec![
        ("StreamARN", stream_arn_field().not_null()),
        (
            "StreamModeDetails",
            FieldDef::new(FieldType::Structure {
                children: vec![(
                    "StreamMode".to_string(),
                    FieldDef::new(FieldType::String)
                        .not_null()
                        .enum_values(vec!["PROVISIONED", "ON_DEMAND"]),
                )],
            })
            .not_null(),
        ),
    ]
}

pub fn put_resource_policy() -> Vec<(&'static str, FieldDef)> {
    vec![
        ("Policy", FieldDef::new(FieldType::String).not_null()),
        ("ResourceARN", stream_arn_field().not_null()),
    ]
}

pub fn get_resource_policy() -> Vec<(&'static str, FieldDef)> {
    vec![("ResourceARN", stream_arn_field().not_null())]
}

pub fn delete_resource_policy() -> Vec<(&'static str, FieldDef)> {
    vec![("ResourceARN", stream_arn_field().not_null())]
}

pub fn tag_resource() -> Vec<(&'static str, FieldDef)> {
    vec![
        ("ResourceARN", stream_arn_field().not_null()),
        (
            "Tags",
            FieldDef::new(FieldType::Map {
                children: Box::new(FieldDef::new(FieldType::String)),
            })
            .not_null()
            .len_gte(1)
            .len_lte(200)
            .child_key_lengths(1, 128)
            .child_value_lengths(0, 256),
        ),
    ]
}

pub fn untag_resource() -> Vec<(&'static str, FieldDef)> {
    vec![
        ("ResourceARN", stream_arn_field().not_null()),
        (
            "TagKeys",
            FieldDef::new(FieldType::List {
                children: Box::new(FieldDef::new(FieldType::String)),
            })
            .not_null()
            .len_gte(1)
            .len_lte(50)
            .child_lengths(1, 128),
        ),
    ]
}

pub fn list_tags_for_resource() -> Vec<(&'static str, FieldDef)> {
    vec![("ResourceARN", stream_arn_field().not_null())]
}

pub fn subscribe_to_shard() -> Vec<(&'static str, FieldDef)> {
    vec![
        (
            "ConsumerARN",
            FieldDef::new(FieldType::String)
                .not_null()
                .len_gte(1)
                .len_lte(2048),
        ),
        (
            "ShardId",
            FieldDef::new(FieldType::String)
                .not_null()
                .regex("[a-zA-Z0-9_.-]+")
                .len_gte(1)
                .len_lte(128),
        ),
        (
            "StartingPosition",
            FieldDef::new(FieldType::Structure {
                children: vec![(
                    "Type".to_string(),
                    FieldDef::new(FieldType::String)
                        .not_null()
                        .enum_values(vec![
                            "AFTER_SEQUENCE_NUMBER",
                            "LATEST",
                            "AT_TIMESTAMP",
                            "AT_SEQUENCE_NUMBER",
                            "TRIM_HORIZON",
                        ]),
                )],
            })
            .not_null(),
        ),
    ]
}

pub fn update_stream_warm_throughput() -> Vec<(&'static str, FieldDef)> {
    vec![
        ("StreamARN", stream_arn_field()),
        (
            "StreamName",
            FieldDef::new(FieldType::String)
                .regex("[a-zA-Z0-9_.-]+")
                .len_gte(1)
                .len_lte(128),
        ),
        (
            "WarmThroughputMiBps",
            FieldDef::new(FieldType::Integer).not_null().gte(0.0),
        ),
    ]
}

pub fn update_max_record_size() -> Vec<(&'static str, FieldDef)> {
    vec![
        ("StreamARN", stream_arn_field().not_null()),
        (
            "MaxRecordSizeInKiB",
            FieldDef::new(FieldType::Integer)
                .not_null()
                .gte(1024.0)
                .lte(10240.0),
        ),
    ]
}

pub fn update_account_settings() -> Vec<(&'static str, FieldDef)> {
    vec![(
        "MinimumThroughputBillingCommitment",
        FieldDef::new(FieldType::Structure {
            children: vec![
                ("Status".to_string(), FieldDef::new(FieldType::String)),
                (
                    "MinimumWriteCapacityUnits".to_string(),
                    FieldDef::new(FieldType::Integer),
                ),
                (
                    "MinimumReadCapacityUnits".to_string(),
                    FieldDef::new(FieldType::Integer),
                ),
            ],
        }),
    )]
}
