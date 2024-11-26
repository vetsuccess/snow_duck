use std::error;

use chrono::{NaiveDate, Datelike};
use duckdb::types::TimeUnit;
use magnus::{RClass, eval, RString};
use once_cell::sync::Lazy;

static TIME_CLASS: Lazy<RClass> = Lazy::new(|| RClass::from_value(eval("Time").unwrap()).unwrap());
static EPOCH_START: Lazy<NaiveDate> = Lazy::new(|| NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
static DATE_CLASS: Lazy<RClass> = Lazy::new(|| RClass::from_value(eval("Date").unwrap()).unwrap());

pub (crate) fn string_from_ruby_hash(input: magnus::RHash, key: &str) -> String {
    // let better_option: magnus::RHash = input.funcall("with_indifferent_access", ()).unwrap();
    // println!("Better option hash is {:?}, trying to get {:?} key", better_option, key);
    RString::from_value(
        input
            .get(key)
            .expect(&format!("{:?} key required to instantiate duckdb", key)),
    )
    .expect(&format!("Value provided as {:?} was not string", key))
    .to_string()
    .unwrap()
}

pub (crate) fn to_standard_column_error(error: &duckdb::Error, column_name: &String) -> magnus::Error {
    to_standard_error(format!("Error converting value of column {} : {}", column_name, error).into())
}

pub (crate) fn to_standard_error(error: Box<dyn error::Error>) -> magnus::Error {
    magnus::Error::new(magnus::exception::standard_error(), error.to_string())
}

pub (crate) fn to_standard_erorr_with_message(error: String) -> magnus::Error {
    magnus::Error::new(magnus::exception::standard_error(), error)
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
        duckdb::types::Value::Interval {..} => todo!(),
        duckdb::types::Value::List(_) => todo!(),
        duckdb::types::Value::Enum(_) => todo!(),
        duckdb::types::Value::Struct(_) => todo!(),
        duckdb::types::Value::Array(_) => todo!(),
        duckdb::types::Value::Map(_) => todo!(),
        duckdb::types::Value::Union(_) => todo!(),
    }
}

#[inline]
fn convert_duck_time(time_unit: TimeUnit, time_value: i64) -> magnus::Value {
    match time_unit {
        duckdb::types::TimeUnit::Second => {
            TIME_CLASS.funcall("at", (time_value, )).unwrap()
        },
        duckdb::types::TimeUnit::Millisecond => {
            let time_value = time_value as f64;
            let divisor = 1000 as i64 as f64;
            let fractional_seconds = time_value / divisor;
            TIME_CLASS.funcall("at", (fractional_seconds, )).unwrap()
        },
        duckdb::types::TimeUnit::Microsecond => {
            let time_value = time_value as f64;
            let divisor = 1000_000 as i64 as f64;
            let fractional_seconds = time_value / divisor;
            TIME_CLASS.funcall("at", (fractional_seconds, )).unwrap()
        },
        duckdb::types::TimeUnit::Nanosecond => {
            let time_value = time_value as f64;
            let divisor = 1000_000_000 as i64 as f64;
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