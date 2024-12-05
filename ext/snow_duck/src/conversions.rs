use std::error;

use chrono::{NaiveDate, Datelike};
use duckdb::{types::{OrderedMap, TimeUnit}, ToSql};
use magnus::{eval, RArray, RClass, RHash, RString};
use once_cell::sync::Lazy;

static TIME_CLASS: Lazy<RClass> = Lazy::new(|| RClass::from_value(eval("Time").unwrap()).unwrap());
static EPOCH_START: Lazy<NaiveDate> = Lazy::new(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
static DATE_CLASS: Lazy<RClass> = Lazy::new(|| RClass::from_value(eval("Date").unwrap()).unwrap());
static DURATION_CLASS: Lazy<RClass> = Lazy::new(|| RClass::from_value(eval("ActiveSupport::Duration").unwrap()).unwrap());
static SECONDS_PER_DAY: Lazy<i64> = Lazy::new(|| eval::<i64>("ActiveSupport::Duration::SECONDS_PER_DAY").unwrap());
static SECONDS_PER_MONTH: Lazy<i64> = Lazy::new(|| eval::<i64>("ActiveSupport::Duration::SECONDS_PER_MONTH").unwrap());
const NANOS_PER_SECOND: i64 = 1_000_000_000;

pub (crate) fn string_from_ruby_hash(input: magnus::RHash, key: &str) -> String {
    // let better_option: magnus::RHash = input.funcall("with_indifferent_access", ()).unwrap();
    // println!("Better option hash is {:?}, trying to get {:?} key", better_option, key);
    RString::from_value(
        input
            .get(key)
            .unwrap_or_else(|| panic!("{:?} key required to instantiate duckdb", key))
    )
    .unwrap_or_else(|| panic!("Value provided as {:?} was not string", key))
    .to_string()
    .unwrap()
}

pub (crate) fn to_standard_column_error(error: &duckdb::Error, column_name: &String) -> magnus::Error {
    to_standard_error(format!("Error converting value of column {} : {}", column_name, error).into())
}

pub (crate) fn to_standard_error(error: Box<dyn error::Error>) -> magnus::Error {
    magnus::Error::new(magnus::exception::standard_error(), error.to_string())
}

#[inline]
pub fn duck_to_ruby(duck_val: duckdb::types::Value) -> magnus::value::Value {
    match duck_val {
        duckdb::types::Value::Null => *magnus::value::QNIL,
        duckdb::types::Value::Boolean(b) => magnus::Value::from(b),
        duckdb::types::Value::TinyInt(i) => magnus::Value::from(i),
        duckdb::types::Value::SmallInt(si) => magnus::Value::from(si),
        duckdb::types::Value::Int(i) => magnus::Value::from(i),
        duckdb::types::Value::BigInt(bi) => magnus::Value::from(bi),
        duckdb::types::Value::HugeInt(hi) => convert_duck_huge_int(hi),
        duckdb::types::Value::UTinyInt(x) => magnus::Value::from(x),
        duckdb::types::Value::USmallInt(x) => magnus::Value::from(x),
        duckdb::types::Value::UInt(x) => magnus::Value::from(x),
        duckdb::types::Value::UBigInt(x) => magnus::Value::from(x),
        duckdb::types::Value::Float(x) => magnus::Value::from(x),
        duckdb::types::Value::Double(x) => magnus::Value::from(x),
        duckdb::types::Value::Decimal(_) => convert_duck_decimal(duck_val),
        duckdb::types::Value::Timestamp(time_unit, time_value) => convert_duck_time(time_unit, time_value),
        duckdb::types::Value::Text(string) => string.into(),
        duckdb::types::Value::Blob(x) => magnus::Value::from(x),
        duckdb::types::Value::Date32(days_since_unix_epoch) => convert_duck_date(days_since_unix_epoch),
        duckdb::types::Value::Time64(time_unit, time_value) => convert_duck_time(time_unit, time_value),
        duckdb::types::Value::Interval { months, days, nanos } => convert_duck_interval(months, days, nanos),
        duckdb::types::Value::List(list) => convert_vector_to_array(list),
        duckdb::types::Value::Enum(value) => magnus::Symbol::new(value.to_string().as_str()).as_value(),
        duckdb::types::Value::Struct(fields) => convert_to_hash(fields),
        duckdb::types::Value::Array(array) => convert_vector_to_array(array),
        duckdb::types::Value::Map(map) => convert_duck_map(map),
        duckdb::types::Value::Union(value) => duck_to_ruby(*value),
    }
}

#[inline]
fn convert_to_hash(map: OrderedMap<String, duckdb::types::Value>) -> magnus::Value
{
    let hash = RHash::new();
    map.iter().for_each(|(key, value)| {
        hash.aset::<magnus::Value, _>(key.clone().into(), duck_to_ruby(value.clone())).expect("Failed to set hash value");
    });
    hash.as_value()
}

#[inline]
fn convert_duck_map(map: OrderedMap<duckdb::types::Value, duckdb::types::Value>) -> magnus::Value {

    let hash = RHash::new();
    map.iter().for_each(|(key, value)| {
        hash.aset::<magnus::Value, _>(duck_to_ruby(key.clone()), duck_to_ruby(value.clone())).expect("Failed to set hash value");
    });
    hash.as_value()
}

#[inline]
fn convert_vector_to_array(duck_vec: Vec<duckdb::types::Value>) -> magnus::Value {
    let ruby_array = RArray::with_capacity(duck_vec.len());
    duck_vec.into_iter().for_each(|value| {
        ruby_array.push(duck_to_ruby(value)).expect("Cound not push value to array");
    });
    ruby_array.as_value()
}

#[inline]
fn convert_duck_time(time_unit: TimeUnit, time_value: i64) -> magnus::Value {
    match time_unit {
        duckdb::types::TimeUnit::Second => {
            TIME_CLASS.funcall("at", (time_value, )).unwrap()
        },
        duckdb::types::TimeUnit::Millisecond => {
            let time_value = time_value as f64;
            let divisor = 1000_f64;
            let fractional_seconds = time_value / divisor;
            TIME_CLASS.funcall("at", (fractional_seconds, )).unwrap()
        },
        duckdb::types::TimeUnit::Microsecond => {
            let time_value = time_value as f64;
            let divisor = 1_000_000_f64;
            let fractional_seconds = time_value / divisor;
            TIME_CLASS.funcall("at", (fractional_seconds, )).unwrap()
        },
        duckdb::types::TimeUnit::Nanosecond => {
            let time_value = time_value as f64;
            let divisor = 1_000_000_000_f64;
            let fractional_seconds = time_value / divisor;
            TIME_CLASS.funcall("at", (fractional_seconds, )).unwrap()
        },
    }
}

#[inline]
fn convert_duck_date(number_of_days: i32) -> magnus::Value {
    let date = *EPOCH_START + chrono::Duration::days(number_of_days.into());
    let (day, month, year) = (date.day(), date.month(), date.year());
    DATE_CLASS.new_instance((year, month, day)).unwrap()
}

#[inline]
fn convert_duck_huge_int(value: i128) -> magnus::Value {
    RString::new(&value.to_string()).funcall("to_i", ()).unwrap()
}

#[inline]
fn convert_duck_decimal(value: duckdb::types::Value) -> magnus::Value {
    // not sure why this is needed, but I can't seem to use plain-old Decimal as f-on parameter
    match value {
        duckdb::types::Value::Decimal(d) =>  {
            let klass = RClass::from_value(eval("BigDecimal").unwrap()).unwrap();
            klass.new_instance((d.to_string().as_str(),)).unwrap()
        }
        _ => panic!("Converting value to decimal, which is not decimal")
    }
}

#[inline]
fn convert_duck_interval(months: i32, days: i32, nanos: i64) -> magnus::Value {
    let month_seconds = months as i64 * *SECONDS_PER_MONTH;
    let day_seconds = days as i64 * *SECONDS_PER_DAY;
    let nano_seconds = nanos / NANOS_PER_SECOND;
    let total_seconds = month_seconds + day_seconds + nano_seconds;
    DURATION_CLASS.funcall("seconds", (total_seconds,)).unwrap()
}