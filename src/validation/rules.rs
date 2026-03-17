use super::{FieldDef, FieldType};

fn stream_name_field() -> FieldDef {
    FieldDef::new(FieldType::String)
        .not_null()
        .regex("[a-zA-Z0-9_.-]+")
        .len_gte(1)
        .len_lte(128)
}

pub fn create_stream() -> Vec<(&'static str, FieldDef)> {
    vec![
        (
            "ShardCount",
            FieldDef::new(FieldType::Integer).not_null().gte(1.0).lte(100000.0),
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
        (
            "NextToken",
            FieldDef::new(FieldType::String).len_gte(1),
        ),
        ("StreamCreationTimestamp", FieldDef::new(FieldType::Timestamp)),
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
        ("StreamName", stream_name_field()),
    ]
}

pub fn put_records() -> Vec<(&'static str, FieldDef)> {
    vec![
        (
            "Records",
            FieldDef::new(FieldType::List {
                children: Box::new(
                    FieldDef::new(FieldType::Structure {
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
                    })
                ),
            })
            .not_null()
            .len_gte(1)
            .len_lte(500)
            .member_str("com.amazonaws.kinesis.v20131202.PutRecordsRequestEntry@c965e310"),
        ),
        ("StreamName", stream_name_field()),
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
            FieldDef::new(FieldType::String).not_null().enum_values(vec![
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
            FieldDef::new(FieldType::Integer).not_null().gte(1.0).lte(168.0),
        ),
        ("StreamName", stream_name_field()),
    ]
}

pub fn decrease_stream_retention_period() -> Vec<(&'static str, FieldDef)> {
    vec![
        (
            "RetentionPeriodHours",
            FieldDef::new(FieldType::Integer).not_null().gte(1.0).lte(168.0),
        ),
        ("StreamName", stream_name_field()),
    ]
}
