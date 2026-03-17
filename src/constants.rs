// AWS Kinesis error types
pub const INVALID_ARGUMENT: &str = "InvalidArgumentException";
pub const RESOURCE_NOT_FOUND: &str = "ResourceNotFoundException";
pub const RESOURCE_IN_USE: &str = "ResourceInUseException";
pub const LIMIT_EXCEEDED: &str = "LimitExceededException";
pub const SERIALIZATION_EXCEPTION: &str = "SerializationException";
pub const VALIDATION_EXCEPTION: &str = "ValidationException";
pub const UNKNOWN_OPERATION: &str = "UnknownOperationException";

// Common JSON field names
pub const STREAM_NAME: &str = "StreamName";
pub const STREAM_ARN: &str = "StreamARN";
pub const RESOURCE_ARN: &str = "ResourceARN";
pub const CONSUMER_NAME: &str = "ConsumerName";
pub const CONSUMER_ARN: &str = "ConsumerARN";
pub const SHARD_ID: &str = "ShardId";
pub const PARTITION_KEY: &str = "PartitionKey";
pub const LIMIT: &str = "Limit";
pub const MAX_RESULTS: &str = "MaxResults";
pub const NEXT_TOKEN: &str = "NextToken";
pub const TAGS: &str = "Tags";
pub const TAG_KEYS: &str = "TagKeys";
pub const POLICY: &str = "Policy";
pub const KEY_ID: &str = "KeyId";
pub const ENCRYPTION_TYPE: &str = "EncryptionType";
pub const RETENTION_PERIOD_HOURS: &str = "RetentionPeriodHours";
pub const SHARD_LEVEL_METRICS: &str = "ShardLevelMetrics";
pub const SHARD_ITERATOR: &str = "ShardIterator";
pub const SHARD_ITERATOR_TYPE: &str = "ShardIteratorType";
pub const RECORDS: &str = "Records";
pub const DATA: &str = "Data";
pub const EXPLICIT_HASH_KEY: &str = "ExplicitHashKey";
pub const SEQUENCE_NUMBER_FOR_ORDERING: &str = "SequenceNumberForOrdering";
pub const STARTING_SEQUENCE_NUMBER: &str = "StartingSequenceNumber";
pub const TARGET_SHARD_COUNT: &str = "TargetShardCount";
pub const STREAM_MODE_DETAILS: &str = "StreamModeDetails";
pub const SCALING_TYPE: &str = "ScalingType";
