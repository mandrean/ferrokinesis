//! Kinesis API operation identifiers.

use crate::validation::{self, FieldDef};
use alloc::vec;
use alloc::vec::Vec;
use core::fmt;
use core::str::FromStr;

macro_rules! define_operations {
    (
        $(
            $(#[$meta:meta])*
            $variant:ident => $name:literal,
        )+
    ) => {
        /// All valid Kinesis API operations.
        ///
        /// Adding a variant here causes the compiler to flag any exhaustiveness gaps
        /// in `dispatch` and `validation_rules` — never use wildcard `_` arms.
        ///
        /// Each variant corresponds to the operation name from the `X-Amz-Target` header
        /// (e.g. `Kinesis_20131202.PutRecord`).
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum Operation {
            $(
                $(#[$meta])*
                $variant,
            )+
        }

        impl Operation {
            /// Number of supported Kinesis operations.
            pub const COUNT: usize = define_operations!(@count $($variant),+);

            /// Stable list of every supported Kinesis operation.
            pub const ALL: [Self; Self::COUNT] = [
                $(Self::$variant,)+
            ];

            /// Returns the canonical operation name used in `X-Amz-Target`.
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $name,)+
                }
            }
        }

        impl FromStr for Operation {
            type Err = ();

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s {
                    $($name => Ok(Self::$variant),)+
                    _ => Err(()),
                }
            }
        }
    };
    (@count $($variant:ident),+) => {
        <[()]>::len(&[$(define_operations!(@replace $variant)),+])
    };
    (@replace $_variant:ident) => { () };
}

define_operations! {
    /// Adds or updates tags on a stream.
    #[doc(alias = "Kinesis_20131202.AddTagsToStream")]
    AddTagsToStream => "AddTagsToStream",
    /// Creates a new Kinesis data stream.
    #[doc(alias = "Kinesis_20131202.CreateStream")]
    CreateStream => "CreateStream",
    /// Decreases the stream's data retention period.
    #[doc(alias = "Kinesis_20131202.DecreaseStreamRetentionPeriod")]
    DecreaseStreamRetentionPeriod => "DecreaseStreamRetentionPeriod",
    /// Deletes a resource-based policy from a stream or consumer.
    #[doc(alias = "Kinesis_20131202.DeleteResourcePolicy")]
    DeleteResourcePolicy => "DeleteResourcePolicy",
    /// Deletes a Kinesis data stream and all its shards and data.
    #[doc(alias = "Kinesis_20131202.DeleteStream")]
    DeleteStream => "DeleteStream",
    /// Deregisters an enhanced fan-out consumer from a stream.
    #[doc(alias = "Kinesis_20131202.DeregisterStreamConsumer")]
    DeregisterStreamConsumer => "DeregisterStreamConsumer",
    /// Returns the limits for the current account and region.
    #[doc(alias = "Kinesis_20131202.DescribeAccountSettings")]
    DescribeAccountSettings => "DescribeAccountSettings",
    /// Describes the shard limits and usage for the account.
    #[doc(alias = "Kinesis_20131202.DescribeLimits")]
    DescribeLimits => "DescribeLimits",
    /// Returns detailed information about a stream, including its shards.
    #[doc(alias = "Kinesis_20131202.DescribeStream")]
    DescribeStream => "DescribeStream",
    /// Returns detailed information about a registered consumer.
    #[doc(alias = "Kinesis_20131202.DescribeStreamConsumer")]
    DescribeStreamConsumer => "DescribeStreamConsumer",
    /// Returns a summary of the stream without shard-level detail.
    #[doc(alias = "Kinesis_20131202.DescribeStreamSummary")]
    DescribeStreamSummary => "DescribeStreamSummary",
    /// Disables enhanced shard-level CloudWatch metrics for a stream.
    #[doc(alias = "Kinesis_20131202.DisableEnhancedMonitoring")]
    DisableEnhancedMonitoring => "DisableEnhancedMonitoring",
    /// Enables enhanced shard-level CloudWatch metrics for a stream.
    #[doc(alias = "Kinesis_20131202.EnableEnhancedMonitoring")]
    EnableEnhancedMonitoring => "EnableEnhancedMonitoring",
    /// Gets data records from a shard using a shard iterator.
    #[doc(alias = "Kinesis_20131202.GetRecords")]
    GetRecords => "GetRecords",
    /// Returns the resource-based policy for a stream or consumer.
    #[doc(alias = "Kinesis_20131202.GetResourcePolicy")]
    GetResourcePolicy => "GetResourcePolicy",
    /// Returns a shard iterator for reading records from a shard.
    #[doc(alias = "Kinesis_20131202.GetShardIterator")]
    GetShardIterator => "GetShardIterator",
    /// Increases the stream's data retention period.
    #[doc(alias = "Kinesis_20131202.IncreaseStreamRetentionPeriod")]
    IncreaseStreamRetentionPeriod => "IncreaseStreamRetentionPeriod",
    /// Lists the shards in a stream and provides information about each.
    #[doc(alias = "Kinesis_20131202.ListShards")]
    ListShards => "ListShards",
    /// Lists the consumers registered to a stream.
    #[doc(alias = "Kinesis_20131202.ListStreamConsumers")]
    ListStreamConsumers => "ListStreamConsumers",
    /// Lists the Kinesis data streams in the current account and region.
    #[doc(alias = "Kinesis_20131202.ListStreams")]
    ListStreams => "ListStreams",
    /// Lists the tags for a Kinesis resource (stream or consumer).
    #[doc(alias = "Kinesis_20131202.ListTagsForResource")]
    ListTagsForResource => "ListTagsForResource",
    /// Lists the tags for a stream (legacy operation; prefer `ListTagsForResource`).
    #[doc(alias = "Kinesis_20131202.ListTagsForStream")]
    ListTagsForStream => "ListTagsForStream",
    /// Merges two adjacent shards in a stream into a single shard.
    #[doc(alias = "Kinesis_20131202.MergeShards")]
    MergeShards => "MergeShards",
    /// Writes a single data record into a stream.
    #[doc(alias = "Kinesis_20131202.PutRecord")]
    PutRecord => "PutRecord",
    /// Writes multiple data records into a stream in a single call.
    #[doc(alias = "Kinesis_20131202.PutRecords")]
    PutRecords => "PutRecords",
    /// Attaches a resource-based policy to a stream or consumer.
    #[doc(alias = "Kinesis_20131202.PutResourcePolicy")]
    PutResourcePolicy => "PutResourcePolicy",
    /// Registers an enhanced fan-out consumer with a stream.
    #[doc(alias = "Kinesis_20131202.RegisterStreamConsumer")]
    RegisterStreamConsumer => "RegisterStreamConsumer",
    /// Removes tags from a stream.
    #[doc(alias = "Kinesis_20131202.RemoveTagsFromStream")]
    RemoveTagsFromStream => "RemoveTagsFromStream",
    /// Splits a shard into two new shards.
    #[doc(alias = "Kinesis_20131202.SplitShard")]
    SplitShard => "SplitShard",
    /// Enables server-side encryption using KMS for a stream.
    #[doc(alias = "Kinesis_20131202.StartStreamEncryption")]
    StartStreamEncryption => "StartStreamEncryption",
    /// Disables server-side encryption for a stream.
    #[doc(alias = "Kinesis_20131202.StopStreamEncryption")]
    StopStreamEncryption => "StopStreamEncryption",
    /// Subscribes to receive data records from a shard via HTTP/2 event stream.
    #[doc(alias = "Kinesis_20131202.SubscribeToShard")]
    SubscribeToShard => "SubscribeToShard",
    /// Adds or updates tags on a Kinesis resource.
    #[doc(alias = "Kinesis_20131202.TagResource")]
    TagResource => "TagResource",
    /// Removes tags from a Kinesis resource.
    #[doc(alias = "Kinesis_20131202.UntagResource")]
    UntagResource => "UntagResource",
    /// Updates account-level settings.
    #[doc(alias = "Kinesis_20131202.UpdateAccountSettings")]
    UpdateAccountSettings => "UpdateAccountSettings",
    /// Updates the maximum record size for a stream.
    #[doc(alias = "Kinesis_20131202.UpdateMaxRecordSize")]
    UpdateMaxRecordSize => "UpdateMaxRecordSize",
    /// Updates the shard count of a stream.
    #[doc(alias = "Kinesis_20131202.UpdateShardCount")]
    UpdateShardCount => "UpdateShardCount",
    /// Switches a stream between `PROVISIONED` and `ON_DEMAND` capacity modes.
    #[doc(alias = "Kinesis_20131202.UpdateStreamMode")]
    UpdateStreamMode => "UpdateStreamMode",
    /// Updates the warm throughput configuration of a stream.
    #[doc(alias = "Kinesis_20131202.UpdateStreamWarmThroughput")]
    UpdateStreamWarmThroughput => "UpdateStreamWarmThroughput",
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Operation {
    /// Returns the validation rules for this operation.
    pub fn validation_rules(&self) -> Vec<(&'static str, FieldDef)> {
        use validation::rules;
        match self {
            Operation::AddTagsToStream => rules::add_tags_to_stream(),
            Operation::CreateStream => rules::create_stream(),
            Operation::DecreaseStreamRetentionPeriod => rules::decrease_stream_retention_period(),
            Operation::DeleteResourcePolicy => rules::delete_resource_policy(),
            Operation::DeleteStream => rules::delete_stream(),
            Operation::DeregisterStreamConsumer => rules::deregister_stream_consumer(),
            Operation::DescribeAccountSettings => vec![],
            Operation::DescribeLimits => vec![],
            Operation::DescribeStream => rules::describe_stream(),
            Operation::DescribeStreamConsumer => rules::describe_stream_consumer(),
            Operation::DescribeStreamSummary => rules::describe_stream_summary(),
            Operation::DisableEnhancedMonitoring => rules::disable_enhanced_monitoring(),
            Operation::EnableEnhancedMonitoring => rules::enable_enhanced_monitoring(),
            Operation::GetRecords => rules::get_records(),
            Operation::GetResourcePolicy => rules::get_resource_policy(),
            Operation::GetShardIterator => rules::get_shard_iterator(),
            Operation::IncreaseStreamRetentionPeriod => rules::increase_stream_retention_period(),
            Operation::ListShards => rules::list_shards(),
            Operation::ListStreamConsumers => rules::list_stream_consumers(),
            Operation::ListStreams => rules::list_streams(),
            Operation::ListTagsForResource => rules::list_tags_for_resource(),
            Operation::ListTagsForStream => rules::list_tags_for_stream(),
            Operation::MergeShards => rules::merge_shards(),
            Operation::PutRecord => rules::put_record(),
            Operation::PutRecords => rules::put_records(),
            Operation::PutResourcePolicy => rules::put_resource_policy(),
            Operation::RegisterStreamConsumer => rules::register_stream_consumer(),
            Operation::RemoveTagsFromStream => rules::remove_tags_from_stream(),
            Operation::SplitShard => rules::split_shard(),
            Operation::StartStreamEncryption => rules::start_stream_encryption(),
            Operation::StopStreamEncryption => rules::stop_stream_encryption(),
            Operation::SubscribeToShard => rules::subscribe_to_shard(),
            Operation::TagResource => rules::tag_resource(),
            Operation::UntagResource => rules::untag_resource(),
            Operation::UpdateAccountSettings => rules::update_account_settings(),
            Operation::UpdateMaxRecordSize => rules::update_max_record_size(),
            Operation::UpdateShardCount => rules::update_shard_count(),
            Operation::UpdateStreamMode => rules::update_stream_mode(),
            Operation::UpdateStreamWarmThroughput => rules::update_stream_warm_throughput(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Operation;
    use alloc::vec::Vec;
    use core::str::FromStr;

    #[test]
    fn all_operations_round_trip_through_canonical_name() {
        let mut seen = Vec::new();

        assert_eq!(Operation::ALL.len(), Operation::COUNT);

        for operation in Operation::ALL {
            let name = operation.as_str();

            assert_eq!(Operation::from_str(name), Ok(operation));
            assert!(
                !seen.contains(&name),
                "duplicate operation name in catalog: {name}"
            );

            seen.push(name);
        }
    }

    #[test]
    fn rejects_unknown_operation_names() {
        assert_eq!(
            Operation::from_str("DefinitelyNotAKinesisOperation"),
            Err(())
        );
    }
}
