use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::sequence;
use crate::shard_iterator;
use crate::store::Store;
use crate::types::ResponseRecord;
use crate::util::current_time_ms;
use serde_json::{Value, json};

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let iterator_str = data[constants::SHARD_ITERATOR].as_str().unwrap_or("");
    let limit = data[constants::LIMIT].as_u64().unwrap_or(10000) as usize;
    let now = current_time_ms();

    let (iterator_time, stream_name, shard_id, seq_no) =
        shard_iterator::decode_shard_iterator(iterator_str)
            .map_err(|_| invalid_shard_iterator())?;

    // Validate shard ID format
    let shard_ix: i64 = {
        let parts: Vec<&str> = shard_id.split('-').collect();
        if parts.len() != 2 || parts[0] != "shardId" {
            return Err(invalid_shard_iterator());
        }
        let ix_str = parts[1];
        if ix_str.len() != 12 || !ix_str.chars().all(|c| c.is_ascii_digit()) {
            return Err(invalid_shard_iterator());
        }
        ix_str.parse().map_err(|_| invalid_shard_iterator())?
    };

    if !(0..2147483648).contains(&shard_ix) {
        return Err(invalid_shard_iterator());
    }

    if iterator_time == 0 || iterator_time > now {
        return Err(invalid_shard_iterator());
    }

    // Validate stream name
    if stream_name.is_empty() || stream_name.len() > 128 {
        return Err(invalid_shard_iterator());
    }
    let re = regex::Regex::new(r"^[a-zA-Z0-9_.\-]+$").unwrap();
    if !re.is_match(&stream_name) {
        return Err(invalid_shard_iterator());
    }

    // Check expiry (configurable TTL)
    let ttl_ms = store.options.iterator_ttl_seconds.saturating_mul(1000);
    if now - iterator_time > ttl_ms {
        return Err(KinesisErrorResponse::client_error(
            constants::EXPIRED_ITERATOR,
            Some(&format!(
                "Iterator expired. The iterator was created at time {} while right now it is {} \
                 which is further in the future than the tolerated delay of {} milliseconds.",
                to_amz_utc_string(iterator_time),
                to_amz_utc_string(now),
                ttl_ms
            )),
        ));
    }

    let seq_obj = sequence::parse_sequence(&seq_no).map_err(|_| invalid_shard_iterator())?;

    let stream = store.get_stream(&stream_name).await.map_err(|mut err| {
        if err.body.error_type == constants::RESOURCE_NOT_FOUND {
            err.body.message = Some(format!(
                "Shard {} in stream {} under account {} does not exist",
                shard_id, stream_name, store.aws_account_id
            ));
        }
        err
    })?;

    if shard_ix >= stream.shards.len() as i64 {
        return Err(KinesisErrorResponse::shard_not_found(
            &shard_id,
            &stream_name,
            &store.aws_account_id,
        ));
    }

    let cutoff_time = now - (stream.retention_period_hours as u64 * 60 * 60 * 1000);
    let cutoff_timestamp = (cutoff_time / 1000) as f64;

    // Record keys are ordered as "{shard_hex}/{seq_num}". Scanning the half-open
    // range [hex(shard_ix), hex(shard_ix+1)) captures exactly the records for this
    // shard in sequence order without a full-table scan or per-record shard check.
    let range_start = format!("{}/{}", sequence::shard_ix_to_hex(shard_ix), seq_no);
    let range_end = sequence::shard_ix_to_hex(shard_ix + 1);
    let range_records = store
        .get_records_range_limited(&stream_name, &range_start, &range_end, limit)
        .await;

    let mut items: Vec<ResponseRecord<'_>> = Vec::with_capacity(range_records.len());
    let mut last_seq_num: Option<&str> = None;
    let mut keys_to_delete = Vec::new();

    for (key, record) in &range_records {
        let seq_num = key.split('/').nth(1).unwrap_or("");

        if record.approximate_arrival_timestamp < cutoff_timestamp {
            keys_to_delete.push(key.clone());
            continue;
        }

        items.push(ResponseRecord {
            partition_key: &record.partition_key,
            data: &record.data,
            approximate_arrival_timestamp: record.approximate_arrival_timestamp,
            sequence_number: seq_num,
        });

        last_seq_num = Some(seq_num);
    }

    // If the iterator's embedded seq_time is in the future (e.g. a LATEST iterator
    // created just before the system clock stepped backward), use that future time as
    // the base for the next-iterator sequence rather than `now`. Using `now` would
    // regress the iterator position and cause the caller to re-scan already-seen records.
    let default_time = if seq_obj.seq_time.unwrap_or(0) > now {
        seq_obj.seq_time.unwrap_or(now)
    } else {
        now
    };

    let next_seq = match last_seq_num.and_then(|s| sequence::parse_sequence(s).ok()) {
        Some(ref last) => sequence::increment_sequence(last, None),
        None => sequence::increment_sequence(&seq_obj, Some(default_time)),
    };

    let mut next_shard_iterator = Some(shard_iterator::create_shard_iterator(
        &stream_name,
        &shard_id,
        &next_seq,
        crate::util::current_time_ms(),
    ));
    let mut millis_behind = 0u64;

    // Per Kinesis spec, NextShardIterator must be absent (not null) once a consumer
    // has read past the end of a closed shard. The three conditions guard exactly that:
    //   1. no records were returned (the consumer is at or past the frontier),
    //   2. the shard has an ending_sequence_number (it is closed), and
    //   3. the iterator's position is at or past the closing sequence's timestamp.
    // Only when all three hold is it safe to omit the next iterator.
    if items.is_empty()
        && let Some(ref end_seq) = stream.shards[shard_ix as usize]
            .sequence_number_range
            .ending_sequence_number
        && let Ok(end_seq_obj) = sequence::parse_sequence(end_seq)
        && seq_obj.seq_time.unwrap_or(0) >= end_seq_obj.seq_time.unwrap_or(0)
    {
        next_shard_iterator = None;
        millis_behind = now.saturating_sub(end_seq_obj.seq_time.unwrap_or(0));
    }

    // Expired-record deletion runs asynchronously so it does not block the caller's
    // response. The records were already excluded from `items` above; this task just
    // reclaims storage space after the response has been sent.
    if !keys_to_delete.is_empty() {
        let store_clone = store.clone();
        let name = stream_name.to_string();
        tokio::spawn(async move {
            store_clone.delete_record_keys(&name, &keys_to_delete).await;
        });
    }

    let record_count = items.len();
    let mut result = json!({
        "MillisBehindLatest": millis_behind,
        "Records": items,
    });

    if let Some(iter) = next_shard_iterator {
        result["NextShardIterator"] = json!(iter);
    }

    tracing::trace!(stream = %stream_name, shard = %shard_id, records = record_count, millis_behind, "records retrieved");
    Ok(Some(result))
}

fn invalid_shard_iterator() -> KinesisErrorResponse {
    KinesisErrorResponse::client_error(constants::INVALID_ARGUMENT, Some("Invalid ShardIterator."))
}

fn to_amz_utc_string(millis: u64) -> String {
    // Format: "Thu Jan 22 01:22:02 UTC 2015"
    let secs = (millis / 1000) as i64;
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;

    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Simple date calculation
    let (year, month, day, weekday) = days_to_date(days_since_epoch);

    let day_names = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
    let month_names = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];

    format!(
        "{} {} {:02} {:02}:{:02}:{:02} UTC {}",
        day_names[weekday as usize],
        month_names[(month - 1) as usize],
        day,
        hours,
        minutes,
        seconds,
        year
    )
}

fn days_to_date(days: i64) -> (i64, i64, i64, i64) {
    // Algorithm from http://howardhinnant.github.io/date_algorithms.html
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    let weekday = ((days + 3) % 7 + 7) % 7; // 0 = Thursday (epoch was Thursday)

    (y, m, d, weekday)
}
