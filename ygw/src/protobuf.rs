// protobuf definitions
use core::f32;

use self::ygw::{value, ParameterValue, Timestamp, Value};

pub mod pvalue {
    include!(concat!(env!("OUT_DIR"), "/yamcs.protobuf.pvalue.rs"));
}

pub mod ygw {
    include!(concat!(env!("OUT_DIR"), "/yamcs.protobuf.ygw.rs"));
}

pub trait IntoValue {
    fn into_value(self) -> Value;
}

impl IntoValue for f32 {
    fn into_value(self) -> Value {
        Value {
            v: Some(value::V::FloatValue(self)),
        }
    }
}
impl IntoValue for f64 {
    fn into_value(self) -> Value {
        Value {
            v: Some(value::V::DoubleValue(self)),
        }
    }
}

impl IntoValue for u32 {
    fn into_value(self) -> Value {
        Value {
            v: Some(value::V::Uint32Value(self)),
        }
    }
}

impl IntoValue for u64 {
    fn into_value(self) -> Value {
        Value {
            v: Some(value::V::Uint64Value(self)),
        }
    }
}

impl IntoValue for bool {
    fn into_value(self) -> Value {
        Value {
            v: Some(value::V::BooleanValue(self)),
        }
    }
}

/// returns a parameter value having only the generation time and the engineering value
pub fn get_pv_eng<T>(id: u32, gentime: Option<Timestamp>, eng_value: T) -> ParameterValue
where
    T: IntoValue,
{
    ParameterValue {
        id,
        raw_value: None,
        eng_value: Some(get_value(eng_value)),
        acquisition_time: None,
        generation_time: gentime,
        acquisition_status: None,
        monitoring_result: None,
        range_condition: None,
        expire_millis: None,
    }
}

/// returns a value for one of the common types
pub fn get_value<T>(v: T) -> Value
where
    T: IntoValue,
{
    v.into_value()
}
