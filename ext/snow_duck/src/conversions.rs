use std::error;

use chrono::{NaiveDate, Datelike};
use duckdb::{types::{OrderedMap, TimeUnit}, ToSql};
use magnus::{eval, value::ReprValue, Class, IntoValue, RArray, RClass, RHash, RString, Ruby};

static TIME_CLASS: magnus::value::Lazy<RClass> = magnus::value::Lazy::new(|ruby| ruby.class_time());
static EPOCH_START: once_cell::sync::Lazy<NaiveDate> = once_cell::sync::Lazy::new(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
static DATE_CLASS: magnus::value::Lazy<RClass> = magnus::value::Lazy::new(|_| RClass::from_value(eval("Date").unwrap()).unwrap());
static DURATION_CLASS: magnus::value::Lazy<RClass> = magnus::value::Lazy::new(|_| RClass::from_value(eval("ActiveSupport::Duration").unwrap()).unwrap());
static SECONDS_PER_DAY: once_cell::sync::Lazy<i64> = once_cell::sync::Lazy::new(|| eval::<i64>("ActiveSupport::Duration::SECONDS_PER_DAY").unwrap());
static SECONDS_PER_MONTH: once_cell::sync::Lazy<i64> = once_cell::sync::Lazy::new(|| eval::<i64>("ActiveSupport::Duration::SECONDS_PER_MONTH").unwrap());

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
        duckdb::types::Value::Null => magnus::value::qnil().as_value(),
        duckdb::types::Value::Boolean(b) => b.into_value(),
        duckdb::types::Value::TinyInt(i) => i.into_value(),
        duckdb::types::Value::SmallInt(si) => si.into_value(),
        duckdb::types::Value::Int(i) => i.into_value(),
        duckdb::types::Value::BigInt(bi) => bi.into_value(),
        duckdb::types::Value::HugeInt(hi) => RString::new(&hi.to_string()).funcall("to_i", ()).unwrap(),
        duckdb::types::Value::UTinyInt(x) => x.into_value(),
        duckdb::types::Value::USmallInt(x) => x.into_value(),
        duckdb::types::Value::UInt(x) => x.into_value(),
        duckdb::types::Value::UBigInt(x) => x.into_value(),
        duckdb::types::Value::Float(x) => x.into_value(),
        duckdb::types::Value::Double(x) => x.into_value(),
        duckdb::types::Value::Decimal(d) => eval(format!("BigDecimal(\"{}\")", d.to_string()).as_str()).unwrap(),
        duckdb::types::Value::Timestamp(time_unit, time_value) => convert_duck_time(time_unit, time_value),
        duckdb::types::Value::Text(string) => string.into_value(),
        duckdb::types::Value::Blob(x) => x.into_value(),
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
        hash.aset::<magnus::Value, _>(key.clone().into_value(), duck_to_ruby(value.clone())).expect("Failed to set hash value");
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
    let ruby = Ruby::get().expect("Ruby not initialized!");
    let time_class_unwrapped = ruby.get_inner(&TIME_CLASS);
    let time_unit_amounts = match time_unit {
        duckdb::types::TimeUnit::Second => 1_f64,
        duckdb::types::TimeUnit::Millisecond => 1000_f64,
        duckdb::types::TimeUnit::Microsecond => 1_000_000_f64,
        duckdb::types::TimeUnit::Nanosecond =>  1_000_000_000_f64,
    };
    let time_value = time_value as f64;
    let fractional_seconds = time_value / time_unit_amounts;
    time_class_unwrapped.funcall("at", (fractional_seconds, )).unwrap()
}

#[inline]
fn convert_duck_date(number_of_days: i32) -> magnus::Value {
    let ruby = Ruby::get().expect("Ruby not initialized!");
    let date = *EPOCH_START + chrono::Duration::days(number_of_days.into());
    let (day, month, year) = (date.day(), date.month(), date.year());
    ruby.get_inner(&DATE_CLASS).new_instance((year, month, day)).unwrap()
}

#[inline]
fn convert_duck_interval(months: i32, days: i32, nanos: i64) -> magnus::Value {
    let ruby = Ruby::get().expect("Ruby not initialized!");
    let month_seconds = months as i64 * *SECONDS_PER_MONTH;
    let day_seconds = days as i64 * *SECONDS_PER_DAY;
    let nano_seconds = nanos / NANOS_PER_SECOND;
    let total_seconds = month_seconds + day_seconds + nano_seconds;
    ruby.get_inner(&DURATION_CLASS).funcall("seconds", (total_seconds,)).unwrap()
}