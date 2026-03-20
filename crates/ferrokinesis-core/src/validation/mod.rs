pub mod rules;

use crate::error::KinesisErrorResponse;
use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use serde_json::Value;

#[cfg(feature = "std")]
use std::collections::HashMap;
#[cfg(feature = "std")]
use std::sync::{LazyLock, RwLock};

#[cfg(feature = "std")]
static REGEX_CACHE: LazyLock<RwLock<HashMap<String, regex::Regex>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

fn is_regex_match(pattern: &str, s: &str) -> bool {
    #[cfg(feature = "std")]
    {
        let cache = REGEX_CACHE.read().unwrap_or_else(|e| e.into_inner());
        if let Some(re) = cache.get(pattern) {
            return re.is_match(s);
        }
        // Benign race: another thread may have inserted this entry
        // between dropping the read lock and acquiring the write lock.
        // entry() deduplicates, so at worst we compile the regex twice.
        drop(cache);
        let mut cache = REGEX_CACHE.write().unwrap_or_else(|e| e.into_inner());
        cache
            .entry(pattern.to_string())
            .or_insert_with(|| {
                let anchored = format!("^{pattern}$");
                regex::Regex::new(&anchored)
                    .unwrap_or_else(|e| panic!("invalid regex pattern '{pattern}': {e}"))
            })
            .is_match(s)
    }
    #[cfg(not(feature = "std"))]
    {
        let anchored = format!("^{pattern}$");
        regex::Regex::new(&anchored)
            .map(|re| re.is_match(s))
            .unwrap_or(false)
    }
}

pub fn to_lower_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(c) => c.to_lowercase().to_string() + chars.as_str(),
    }
}

/// Field type descriptor
#[derive(Debug, Clone)]
pub enum FieldType {
    Boolean,
    Short,
    Integer,
    Long,
    Double,
    String,
    Blob,
    Timestamp,
    List {
        children: Box<FieldDef>,
    },
    Map {
        children: Box<FieldDef>,
    },
    Structure {
        children: Vec<(alloc::string::String, FieldDef)>,
    },
}

/// Field definition with type and validation constraints
#[derive(Debug, Clone)]
pub struct FieldDef {
    pub field_type: FieldType,
    pub required: bool,
    pub not_null: bool,
    pub regex: Option<alloc::string::String>,
    pub greater_than_or_equal: Option<f64>,
    pub less_than_or_equal: Option<f64>,
    pub length_greater_than_or_equal: Option<usize>,
    pub length_less_than_or_equal: Option<usize>,
    pub enum_values: Option<Vec<alloc::string::String>>,
    pub child_lengths: Option<(usize, usize)>,
    pub child_key_lengths: Option<(usize, usize)>,
    pub child_value_lengths: Option<(usize, usize)>,
    pub member_str: Option<alloc::string::String>,
}

impl FieldDef {
    pub fn new(field_type: FieldType) -> Self {
        Self {
            field_type,
            required: false,
            not_null: false,
            regex: None,
            greater_than_or_equal: None,
            less_than_or_equal: None,
            length_greater_than_or_equal: None,
            length_less_than_or_equal: None,
            enum_values: None,
            child_lengths: None,
            child_key_lengths: None,
            child_value_lengths: None,
            member_str: None,
        }
    }

    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    pub fn not_null(mut self) -> Self {
        self.not_null = true;
        self
    }

    pub fn regex(mut self, pattern: &str) -> Self {
        self.regex = Some(pattern.to_string());
        self
    }

    pub fn gte(mut self, val: f64) -> Self {
        self.greater_than_or_equal = Some(val);
        self
    }

    pub fn lte(mut self, val: f64) -> Self {
        self.less_than_or_equal = Some(val);
        self
    }

    pub fn len_gte(mut self, val: usize) -> Self {
        self.length_greater_than_or_equal = Some(val);
        self
    }

    pub fn len_lte(mut self, val: usize) -> Self {
        self.length_less_than_or_equal = Some(val);
        self
    }

    pub fn enum_values(mut self, values: Vec<&str>) -> Self {
        self.enum_values = Some(values.into_iter().map(|s| s.to_string()).collect());
        self
    }

    pub fn child_lengths(mut self, min: usize, max: usize) -> Self {
        self.child_lengths = Some((min, max));
        self
    }

    pub fn child_key_lengths(mut self, min: usize, max: usize) -> Self {
        self.child_key_lengths = Some((min, max));
        self
    }

    pub fn child_value_lengths(mut self, min: usize, max: usize) -> Self {
        self.child_value_lengths = Some((min, max));
        self
    }

    pub fn member_str(mut self, s: &str) -> Self {
        self.member_str = Some(s.to_string());
        self
    }
}

/// Check types of incoming data against expected field definitions.
/// Returns cleaned data with only recognized fields and correct types.
pub fn check_types(
    data: &Value,
    fields: &[(&str, &FieldDef)],
) -> Result<Value, KinesisErrorResponse> {
    let obj = match data.as_object() {
        Some(obj) => obj,
        None => return Ok(Value::Object(serde_json::Map::new())),
    };

    let mut result = serde_json::Map::new();

    for &(field_name, field_def) in fields {
        if let Some(val) = obj.get(field_name) {
            if val.is_null() {
                continue;
            }
            let checked = check_type(val, &field_def.field_type, fields)?;
            if !checked.is_null() {
                result.insert(field_name.to_string(), checked);
            }
        }
    }

    Ok(Value::Object(result))
}

#[allow(clippy::only_used_in_recursion)]
fn check_type(
    val: &Value,
    field_type: &FieldType,
    all_fields: &[(&str, &FieldDef)],
) -> Result<Value, KinesisErrorResponse> {
    if val.is_null() {
        return Ok(Value::Null);
    }

    match field_type {
        FieldType::Boolean => match val {
            Value::Number(_) => Err(type_error(
                "class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to an Boolean",
            )),
            Value::String(s) => Err(type_error(&format!(
                "'{}' can not be converted to an Boolean",
                s.to_uppercase()
            ))),
            Value::Array(_) => Err(type_error("Start of list found where not expected")),
            Value::Object(_) => Err(type_error(
                "Start of structure or map found where not expected.",
            )),
            Value::Bool(_) => Ok(val.clone()),
            _ => Ok(val.clone()),
        },
        FieldType::Short | FieldType::Integer | FieldType::Long | FieldType::Double => {
            let type_name = match field_type {
                FieldType::Short => "Short",
                FieldType::Integer => "Integer",
                FieldType::Long => "Long",
                FieldType::Double => "Double",
                _ => unreachable!(),
            };
            match val {
                Value::Bool(_) => Err(type_error(&format!(
                    "class java.lang.Boolean can not be converted to an {type_name}"
                ))),
                Value::String(_) => Err(type_error(&format!(
                    "class java.lang.String can not be converted to an {type_name}"
                ))),
                Value::Array(_) => Err(type_error("Start of list found where not expected")),
                Value::Object(_) => Err(type_error(
                    "Start of structure or map found where not expected.",
                )),
                Value::Number(n) => {
                    if matches!(field_type, FieldType::Double) {
                        Ok(val.clone())
                    } else {
                        let mut v = n.as_f64().unwrap_or(0.0) as i64;
                        if matches!(field_type, FieldType::Short) {
                            v = v.min(32767);
                        }
                        if matches!(field_type, FieldType::Integer) {
                            v = v.min(2147483647);
                        }
                        Ok(Value::Number(serde_json::Number::from(v)))
                    }
                }
                _ => Ok(val.clone()),
            }
        }
        FieldType::String => match val {
            Value::Bool(_) => Err(type_error(
                "class java.lang.Boolean can not be converted to an String",
            )),
            Value::Number(_) => Err(type_error(
                "class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to an String",
            )),
            Value::Array(_) => Err(type_error("Start of list found where not expected")),
            Value::Object(_) => Err(type_error(
                "Start of structure or map found where not expected.",
            )),
            Value::String(_) => Ok(val.clone()),
            _ => Ok(val.clone()),
        },
        FieldType::Blob => match val {
            Value::Bool(_) => Err(type_error(
                "class java.lang.Boolean can not be converted to a Blob",
            )),
            Value::Number(_) => Err(type_error(
                "class com.amazon.coral.value.json.numbers.TruncatingBigNumber can not be converted to a Blob",
            )),
            Value::Array(_) => Err(type_error("Start of list found where not expected")),
            Value::Object(_) => Err(type_error(
                "Start of structure or map found where not expected.",
            )),
            Value::String(s) => {
                if s.len() % 4 != 0 {
                    return Err(type_error(&format!(
                        "'{s}' can not be converted to a Blob: Base64 encoded length is expected a multiple of 4 bytes but found: {}",
                        s.len()
                    )));
                }
                // Check for invalid base64 characters
                if let Some(m) = find_invalid_base64_char(s) {
                    return Err(type_error(&format!(
                        "'{s}' can not be converted to a Blob: Invalid Base64 character: '{m}'"
                    )));
                }
                // Try to decode and re-encode to verify
                use base64::Engine;
                let decoded = base64::engine::general_purpose::STANDARD.decode(s);
                if let Ok(bytes) = decoded {
                    let reencoded = base64::engine::general_purpose::STANDARD.encode(&bytes);
                    if reencoded != *s {
                        return Err(type_error(&format!(
                            "'{s}' can not be converted to a Blob: Invalid last non-pad Base64 character dectected"
                        )));
                    }
                }
                Ok(val.clone())
            }
            _ => Ok(val.clone()),
        },
        FieldType::Timestamp => match val {
            Value::Bool(_) => Err(type_error(
                "class java.lang.Boolean can not be converted to milliseconds since epoch",
            )),
            Value::String(_) => Err(type_error(
                "class java.lang.String can not be converted to milliseconds since epoch",
            )),
            Value::Array(_) => Err(type_error("Start of list found where not expected")),
            Value::Object(_) => Err(type_error(
                "Start of structure or map found where not expected.",
            )),
            _ => Ok(val.clone()),
        },
        FieldType::List { children } => match val {
            Value::Bool(_) | Value::Number(_) | Value::String(_) => {
                Err(type_error("Expected list or null"))
            }
            Value::Object(_) => Err(type_error(
                "Start of structure or map found where not expected.",
            )),
            Value::Array(arr) => {
                let mut result = Vec::new();
                for item in arr {
                    result.push(check_type(item, &children.field_type, all_fields)?);
                }
                Ok(Value::Array(result))
            }
            _ => Ok(val.clone()),
        },
        FieldType::Map { children } => match val {
            Value::Bool(_) | Value::Number(_) | Value::String(_) => {
                Err(type_error("Expected map or null"))
            }
            Value::Array(_) => Err(type_error("Start of list found where not expected")),
            Value::Object(obj) => {
                let mut result = serde_json::Map::new();
                for (key, value) in obj {
                    result.insert(
                        key.clone(),
                        check_type(value, &children.field_type, all_fields)?,
                    );
                }
                Ok(Value::Object(result))
            }
            _ => Ok(val.clone()),
        },
        FieldType::Structure { children } => match val {
            Value::Bool(_) | Value::Number(_) | Value::String(_) => {
                Err(type_error("Expected null"))
            }
            Value::Array(_) => Err(type_error("Start of list found where not expected")),
            Value::Object(_) => {
                let child_refs: Vec<(&str, &FieldDef)> =
                    children.iter().map(|(k, v)| (k.as_str(), v)).collect();
                check_types(val, &child_refs)
            }
            _ => Ok(val.clone()),
        },
    }
}

fn find_invalid_base64_char(s: &str) -> Option<char> {
    let bytes = s.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'+' | b'/' => {}
            b'=' => {
                // '=' followed by non-'=' is invalid
                if i + 1 < bytes.len() && bytes[i + 1] != b'=' {
                    return Some(bytes[i] as char);
                }
            }
            _ => return Some(b as char),
        }
    }
    None
}

fn type_error(msg: &str) -> KinesisErrorResponse {
    KinesisErrorResponse::serialization_error(msg)
}

/// Check validation constraints on already type-checked data
#[allow(clippy::type_complexity)]
pub fn check_validations(
    data: &Value,
    fields: &[(&str, &FieldDef)],
    custom: Option<&dyn Fn(&Value) -> Option<String>>,
) -> Result<(), KinesisErrorResponse> {
    let obj = data.as_object().unwrap_or(&serde_json::Map::new()).clone();

    // Check required fields first
    for &(field_name, field_def) in fields {
        if field_def.required && !obj.contains_key(field_name) {
            return Err(KinesisErrorResponse::validation_error(&format!(
                "The paramater '{}' is required but was not present in the request",
                to_lower_first(field_name)
            )));
        }
    }

    let mut errors: Vec<String> = Vec::new();

    fn check_non_required(
        attr: &str,
        data: &Value,
        field_def: &FieldDef,
        parent: &str,
        errors: &mut Vec<String>,
    ) {
        if errors.len() >= 10 {
            return;
        }

        // notNull check
        if field_def.not_null && data.is_null() {
            validate(
                false,
                "Member must not be null",
                data,
                &field_def.field_type,
                &field_def.member_str,
                parent,
                attr,
                errors,
            );
        }

        if data.is_null() {
            return;
        }

        // greaterThanOrEqual
        if let Some(min) = field_def.greater_than_or_equal
            && let Some(n) = data.as_f64()
        {
            validate(
                n >= min,
                &format!(
                    "Member must have value greater than or equal to {}",
                    min as i64
                ),
                data,
                &field_def.field_type,
                &field_def.member_str,
                parent,
                attr,
                errors,
            );
        }

        // lessThanOrEqual
        if let Some(max) = field_def.less_than_or_equal
            && let Some(n) = data.as_f64()
        {
            validate(
                n <= max,
                &format!(
                    "Member must have value less than or equal to {}",
                    max as i64
                ),
                data,
                &field_def.field_type,
                &field_def.member_str,
                parent,
                attr,
                errors,
            );
        }

        // regex
        if let Some(ref pattern) = field_def.regex
            && let Some(s) = data.as_str()
        {
            let matched = is_regex_match(pattern, s);
            validate(
                matched,
                &format!("Member must satisfy regular expression pattern: {pattern}"),
                data,
                &field_def.field_type,
                &field_def.member_str,
                parent,
                attr,
                errors,
            );
        }

        // lengthGreaterThanOrEqual
        if let Some(min) = field_def.length_greater_than_or_equal {
            let length = get_data_length(data, &field_def.field_type);
            validate(
                length >= min,
                &format!("Member must have length greater than or equal to {min}"),
                data,
                &field_def.field_type,
                &field_def.member_str,
                parent,
                attr,
                errors,
            );
        }

        // lengthLessThanOrEqual
        if let Some(max) = field_def.length_less_than_or_equal {
            let length = get_data_length(data, &field_def.field_type);
            validate(
                length <= max,
                &format!("Member must have length less than or equal to {max}"),
                data,
                &field_def.field_type,
                &field_def.member_str,
                parent,
                attr,
                errors,
            );
        }

        // enum
        if let Some(ref values) = field_def.enum_values
            && let Some(s) = data.as_str()
        {
            validate(
                values.iter().any(|v| v == s),
                &format!(
                    "Member must satisfy enum value set: [{}]",
                    values.join(", ")
                ),
                data,
                &field_def.field_type,
                &field_def.member_str,
                parent,
                attr,
                errors,
            );
        }

        // childLengths
        if let Some((min, max)) = field_def.child_lengths
            && let Some(arr) = data.as_array()
        {
            let valid = arr.iter().all(|item| {
                if let Some(s) = item.as_str() {
                    s.len() >= min && s.len() <= max
                } else {
                    true
                }
            });
            validate(
                valid,
                &format!(
                    "Member must satisfy constraint: [Member must have length less than or equal to {max}, Member must have length greater than or equal to {min}]"
                ),
                data,
                &field_def.field_type,
                &field_def.member_str,
                parent,
                attr,
                errors,
            );
        }

        // childKeyLengths
        if let Some((min, max)) = field_def.child_key_lengths
            && let Some(obj) = data.as_object()
        {
            let valid = obj.keys().all(|k| k.len() >= min && k.len() <= max);
            validate(
                valid,
                &format!(
                    "Map keys must satisfy constraint: [Member must have length less than or equal to {max}, Member must have length greater than or equal to {min}]"
                ),
                data,
                &field_def.field_type,
                &field_def.member_str,
                parent,
                attr,
                errors,
            );
        }

        // childValueLengths
        if let Some((min, max)) = field_def.child_value_lengths
            && let Some(obj) = data.as_object()
        {
            let valid = obj.values().all(|v| {
                if let Some(s) = v.as_str() {
                    s.len() >= min && s.len() <= max
                } else {
                    true
                }
            });
            validate(
                valid,
                &format!(
                    "Map value must satisfy constraint: [Member must have length less than or equal to {max}, Member must have length greater than or equal to {min}]"
                ),
                data,
                &field_def.field_type,
                &field_def.member_str,
                parent,
                attr,
                errors,
            );
        }

        // Handle children for List/Map/Structure types
        match &field_def.field_type {
            FieldType::List { children } => {
                if let Some(arr) = data.as_array() {
                    for (i, item) in arr.iter().enumerate() {
                        let child_parent = if parent.is_empty() {
                            format!("{}.{}", to_lower_first(attr), i + 1)
                        } else {
                            format!("{parent}.{}.{}", to_lower_first(attr), i + 1)
                        };
                        check_non_required("member", item, children, &child_parent, errors);
                    }
                }
            }
            FieldType::Map { children } => {
                if let Some(obj) = data.as_object() {
                    let keys: Vec<String> = obj.keys().rev().cloned().collect();
                    for key in keys {
                        let child_parent = if parent.is_empty() {
                            format!("{}.{key}", to_lower_first(attr))
                        } else {
                            format!("{parent}.{}.{key}", to_lower_first(attr))
                        };
                        check_non_required("member", &obj[&key], children, &child_parent, errors);
                    }
                }
            }
            FieldType::Structure { children } => {
                if let Some(_obj) = data.as_object() {
                    let child_parent = if parent.is_empty() {
                        to_lower_first(attr)
                    } else {
                        format!("{parent}.{}", to_lower_first(attr))
                    };
                    for (child_name, child_def) in children {
                        let child_val = data.get(child_name).cloned().unwrap_or(Value::Null);
                        check_non_required(
                            child_name,
                            &child_val,
                            child_def,
                            &child_parent,
                            errors,
                        );
                    }
                }
            }
            _ => {}
        }
    }

    for &(field_name, field_def) in fields {
        let val = obj.get(field_name).cloned().unwrap_or(Value::Null);
        check_non_required(field_name, &val, field_def, "", &mut errors);
    }

    if !errors.is_empty() {
        let msg = format!(
            "{} validation error{} detected: {}",
            errors.len(),
            if errors.len() > 1 { "s" } else { "" },
            errors.join("; ")
        );
        return Err(KinesisErrorResponse::validation_error(&msg));
    }

    if let Some(custom_fn) = custom
        && let Some(msg) = custom_fn(data)
    {
        return Err(KinesisErrorResponse::validation_error(&msg));
    }

    Ok(())
}

fn get_data_length(data: &Value, field_type: &FieldType) -> usize {
    match field_type {
        FieldType::Blob => {
            if let Some(s) = data.as_str() {
                let decoded = crate::util::base64_decoded_len(s);
                if decoded > 0 || s.is_empty() {
                    decoded
                } else {
                    s.len()
                }
            } else {
                0
            }
        }
        _ => {
            if let Some(s) = data.as_str() {
                s.len()
            } else if let Some(arr) = data.as_array() {
                arr.len()
            } else if let Some(obj) = data.as_object() {
                obj.len()
            } else {
                0
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn validate(
    predicate: bool,
    msg: &str,
    data: &Value,
    field_type: &FieldType,
    member_str: &Option<String>,
    parent: &str,
    key: &str,
    errors: &mut Vec<String>,
) {
    if predicate {
        return;
    }
    let value = value_str(data, field_type, member_str);
    let value_display = if value == "null" {
        value
    } else {
        format!("'{value}'")
    };
    let parent_prefix = if parent.is_empty() {
        String::new()
    } else {
        format!("{parent}.")
    };
    errors.push(format!(
        "Value {value_display} at '{parent_prefix}{}' failed to satisfy constraint: {msg}",
        to_lower_first(key)
    ));
}

fn value_str(data: &Value, field_type: &FieldType, member_str: &Option<String>) -> String {
    if data.is_null() {
        return "null".to_string();
    }
    match field_type {
        FieldType::Blob => {
            if let Some(s) = data.as_str() {
                let length = crate::util::base64_decoded_len(s);
                format!("java.nio.HeapByteBuffer[pos=0 lim={length} cap={length}]")
            } else {
                "null".to_string()
            }
        }
        FieldType::List { .. } => {
            if let Some(arr) = data.as_array() {
                let items: Vec<String> = arr
                    .iter()
                    .map(|item| {
                        member_str
                            .as_deref()
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| {
                                item.as_str()
                                    .map(|s| s.to_string())
                                    .unwrap_or_else(|| format!("{item}"))
                            })
                    })
                    .collect();
                format!("[{}]", items.join(", "))
            } else {
                "null".to_string()
            }
        }
        FieldType::Map { .. } => {
            if let Some(obj) = data.as_object() {
                let items: Vec<String> = obj
                    .iter()
                    .map(|(k, v)| {
                        let val =
                            member_str
                                .as_deref()
                                .map(|s| s.to_string())
                                .unwrap_or_else(|| {
                                    v.as_str()
                                        .map(|s| s.to_string())
                                        .unwrap_or_else(|| format!("{v}"))
                                });
                        format!("{k}={val}")
                    })
                    .collect();
                format!("{{{}}}", items.join(", "))
            } else {
                "null".to_string()
            }
        }
        _ => {
            if let Some(s) = data.as_str() {
                s.to_string()
            } else if let Some(n) = data.as_i64() {
                n.to_string()
            } else if let Some(n) = data.as_f64() {
                n.to_string()
            } else if let Some(b) = data.as_bool() {
                b.to_string()
            } else {
                format!("{data}")
            }
        }
    }
}
