// protobuf definitions
use core::f32;
use std::time::{SystemTime, UNIX_EPOCH};

use ygw::PreparedCommand;

use crate::YgwError;

use self::ygw::{value, ParameterValue, Timestamp, Value};
/*
pub mod pvalue {
    include!(concat!(env!("OUT_DIR"), "/yamcs.protobuf.pvalue.rs"));
}
*/
pub mod ygw {
    use std::fmt;

    include!(concat!(env!("OUT_DIR"), "/yamcs.protobuf.ygw.rs"));

    impl fmt::Display for PreparedCommand {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            // Customize this to provide a meaningful display of PreparedCommand
            write!(f, "{:?}", self)
        }
    }
}

impl From<f32> for Value {
    fn from(item: f32) -> Self {
        Value {
            v: Some(value::V::FloatValue(item)),
        }
    }
}

impl From<f64> for Value {
    fn from(item: f64) -> Self {
        Value {
            v: Some(value::V::DoubleValue(item)),
        }
    }
}

impl From<u32> for Value {
    fn from(item: u32) -> Self {
        Value {
            v: Some(value::V::Uint32Value(item)),
        }
    }
}

impl From<i32> for Value {
    fn from(item: i32) -> Self {
        Value {
            v: Some(value::V::Sint32Value(item)),
        }
    }
}

impl From<u64> for Value {
    fn from(item: u64) -> Self {
        Value {
            v: Some(value::V::Uint64Value(item)),
        }
    }
}

impl From<i64> for Value {
    fn from(item: i64) -> Self {
        Value {
            v: Some(value::V::Sint64Value(item)),
        }
    }
}
impl From<bool> for Value {
    fn from(item: bool) -> Self {
        Value {
            v: Some(value::V::BooleanValue(item)),
        }
    }
}

impl TryFrom<Value> for f32 {
    type Error = YgwError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value.v {
            Some(value::V::FloatValue(v)) => Ok(v),
            _ => Err(YgwError::ConversionError(
                format!("{:?}", value),
                "f32".into(),
            )),
        }
    }
}

impl TryFrom<Value> for f64 {
    type Error = YgwError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value.v {
            Some(value::V::DoubleValue(v)) => Ok(v),
            _ => Err(YgwError::ConversionError(
                format!("{:?}", value),
                "f64".into(),
            )),
        }
    }
}

impl TryFrom<Value> for u32 {
    type Error = YgwError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value.v {
            Some(value::V::Uint32Value(v)) => Ok(v),
            _ => Err(YgwError::ConversionError(
                format!("{:?}", value),
                "u32".into(),
            )),
        }
    }
}

impl TryFrom<Value> for Vec<u8> {
    type Error = YgwError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value.v {
            Some(value::V::BinaryValue(v)) => Ok(v),
            _ => Err(YgwError::ConversionError(
                format!("{:?}", value),
                "u32".into(),
            )),
        }
    }
}

impl TryFrom<Value> for i32 {
    type Error = YgwError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value.v {
            Some(value::V::Sint32Value(v)) => Ok(v),
            _ => Err(YgwError::ConversionError(
                format!("{:?}", value),
                "i32".into(),
            )),
        }
    }
}

impl TryFrom<Value> for u64 {
    type Error = YgwError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value.v {
            Some(value::V::Uint64Value(v)) => Ok(v),
            _ => Err(YgwError::ConversionError(
                format!("{:?}", value),
                "u64".into(),
            )),
        }
    }
}

impl TryFrom<Value> for i64 {
    type Error = YgwError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value.v {
            Some(value::V::Sint64Value(v)) => Ok(v),
            _ => Err(YgwError::ConversionError(
                format!("{:?}", value),
                "i64".into(),
            )),
        }
    }
}

impl TryFrom<Value> for bool {
    type Error = YgwError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value.v {
            Some(value::V::BooleanValue(v)) => Ok(v),
            _ => Err(YgwError::ConversionError(
                format!("{:?}", value),
                "bool".into(),
            )),
        }
    }
}

impl TryFrom<Value> for String {
    type Error = YgwError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value.v {
            Some(value::V::StringValue(v)) => Ok(v),
            _ => Err(YgwError::ConversionError(
                format!("{:?}", value),
                "String".into(),
            )),
        }
    }
}

/// returns a parameter value having only the generation time and the engineering value
pub fn get_pv_eng<T>(id: u32, gentime: Option<Timestamp>, eng_value: T) -> ParameterValue
where
    T: Into<Value>,
{
    ParameterValue {
        id,
        raw_value: None,
        eng_value: Some(get_value(eng_value)),
        acquisition_time: None,
        generation_time: gentime,
        //  acquisition_status: None,
        // monitoring_result: None,
        //range_condition: None,
        expire_millis: None,
    }
}

/// returns a value for one of the common types
pub fn get_value<T>(v: T) -> Value
where
    T: Into<Value>,
{
    v.into()
}

/// looks up the raw value of the argument arg_name in the command and returns its value if it is of correct type
/// if the argument is not found or has no raw value or has a different type an error is returned
pub fn get_raw_arg<T: TryFrom<Value>>(pc: &PreparedCommand, arg_name: &str) -> Result<T, YgwError> {
    for assignment in &pc.assignments {
        if assignment.name == arg_name {
            if let Some(ref value) = assignment.raw_value {
                return T::try_from(value.clone()).map_err(|_| {
                    YgwError::CommandError(format!(
                        "Argument '{}' found but has incorrect type",
                        arg_name
                    ))
                });
            }
            return Err(YgwError::CommandError(format!(
                "Argument '{}' found but has no raw value",
                arg_name
            )));
        }
    }
    Err(YgwError::CommandError(format!(
        "Argument '{}' not found",
        arg_name
    )))
}

/// looks up the engineering value of the argument arg_name in the command and returns its value if it is of correct type
/// if the argument is not found or has no raw value or has a different type an error is returned
pub fn get_eng_arg<T: TryFrom<Value>>(pc: &PreparedCommand, arg_name: &str) -> Result<T, YgwError> {
    for assignment in &pc.assignments {
        if assignment.name == arg_name {
            if let Some(ref value) = assignment.eng_value {
                return T::try_from(value.clone()).map_err(|_| {
                    YgwError::CommandError(format!(
                        "Argument '{}' found but has incorrect type",
                        arg_name
                    ))
                });
            }
            return Err(YgwError::CommandError(format!(
                "Argument '{}' found but has no engineering value",
                arg_name
            )));
        }
    }
    Err(YgwError::CommandError(format!(
        "Argument '{}' not found",
        arg_name
    )))
}

pub fn now() -> Timestamp {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Cannot use times before the unix epoch");
    let millis =
        since_the_epoch.as_secs() as i64 * 1000 + since_the_epoch.subsec_nanos() as i64 / 1_000_000;
    let millis = millis + 37_000; //leap seconds FIXME
    let picos = (since_the_epoch.subsec_nanos() % 1_000_000) * 1000;
    Timestamp { millis, picos }
}
