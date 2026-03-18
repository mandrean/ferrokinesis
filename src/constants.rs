// AWS Kinesis error types
pub const INVALID_ARGUMENT: &str = "InvalidArgumentException";
pub const RESOURCE_NOT_FOUND: &str = "ResourceNotFoundException";
pub const RESOURCE_IN_USE: &str = "ResourceInUseException";
pub const LIMIT_EXCEEDED: &str = "LimitExceededException";
pub const EXPIRED_ITERATOR: &str = "ExpiredIteratorException";
pub const SERIALIZATION_EXCEPTION: &str = "SerializationException";
pub const VALIDATION_EXCEPTION: &str = "ValidationException";
pub const UNKNOWN_OPERATION: &str = "UnknownOperationException";
pub const INCOMPLETE_SIGNATURE: &str = "IncompleteSignatureException";
pub const MISSING_AUTH_TOKEN: &str = "MissingAuthenticationTokenException";
pub const INVALID_SIGNATURE: &str = "InvalidSignatureException";
pub const ACCESS_DENIED: &str = "AccessDeniedException";
// HTTP content types
pub const CONTENT_TYPE_JSON: &str = "application/x-amz-json-1.1";
pub const CONTENT_TYPE_CBOR: &str = "application/x-amz-cbor-1.1";

// API identifier
pub const KINESIS_API: &str = "Kinesis_20131202";

// Common JSON field names
pub const STREAM_NAME: &str = "StreamName";
pub const STREAM_ARN: &str = "StreamARN";
pub const RESOURCE_ARN: &str = "ResourceARN";
pub const CONSUMER_NAME: &str = "ConsumerName";
pub const CONSUMER_ARN: &str = "ConsumerARN";
pub const SHARD_ID: &str = "ShardId";
pub const SHARD_COUNT: &str = "ShardCount";
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
pub const TIMESTAMP: &str = "Timestamp";
pub const EXCLUSIVE_START_STREAM_NAME: &str = "ExclusiveStartStreamName";
pub const EXCLUSIVE_START_SHARD_ID: &str = "ExclusiveStartShardId";
pub const EXCLUSIVE_START_TAG_KEY: &str = "ExclusiveStartTagKey";
pub const STARTING_POSITION: &str = "StartingPosition";
pub const SHARD_TO_SPLIT: &str = "ShardToSplit";
pub const NEW_STARTING_HASH_KEY: &str = "NewStartingHashKey";
pub const SHARD_TO_MERGE: &str = "ShardToMerge";
pub const ADJACENT_SHARD_TO_MERGE: &str = "AdjacentShardToMerge";
pub const MAX_RECORD_SIZE_IN_KIB: &str = "MaxRecordSizeInKiB";
pub const WARM_THROUGHPUT_MIBPS: &str = "WarmThroughputMiBps";
pub const MINIMUM_THROUGHPUT_BILLING_COMMITMENT: &str = "MinimumThroughputBillingCommitment";

// Encryption type values
pub const ENCRYPTION_KMS: &str = "KMS";
pub const ENCRYPTION_NONE: &str = "NONE";

// Stream mode values
pub const STREAM_MODE_PROVISIONED: &str = "PROVISIONED";
pub const STREAM_MODE_ON_DEMAND: &str = "ON_DEMAND";

// Shard iterator type values
pub const SHARD_ITERATOR_TRIM_HORIZON: &str = "TRIM_HORIZON";
pub const SHARD_ITERATOR_LATEST: &str = "LATEST";
pub const SHARD_ITERATOR_AT_SEQUENCE_NUMBER: &str = "AT_SEQUENCE_NUMBER";
pub const SHARD_ITERATOR_AFTER_SEQUENCE_NUMBER: &str = "AFTER_SEQUENCE_NUMBER";
pub const SHARD_ITERATOR_AT_TIMESTAMP: &str = "AT_TIMESTAMP";
