use std::{
    borrow::BorrowMut, cell::OnceCell, fmt::format, rc::Rc, sync::{Mutex, Once, OnceLock}
};

use criterion::{criterion_group, criterion_main, Criterion};
use magnus::{RHash, Ruby};
use rand::{distributions::Alphanumeric, Rng};
use snow_duck::MutDatabase;

static INIT: Once = Once::new();
static mut CLEANUP: Option<magnus::embed::Cleanup> = None;

fn dates_pluck(c: &mut Criterion) {
    let ruby = get_ruby_vm();
    ruby.require("date").unwrap();

    let db = generate_dates_data(&ruby);
    c.bench_function("pluck dates", |b| {
        b.iter(|| db.duck_pluck("SELECT * FROM dates".to_owned()))
    });
}

fn dates_pluck_to_hash(c: &mut Criterion) {
    let ruby = get_ruby_vm();
    ruby.require("date").unwrap();
    let db = generate_dates_data(&ruby);
    c.bench_function("pluck to hash dates", |b| {
        b.iter(|| db.duck_pluck_to_hash("SELECT * FROM dates".to_owned()))
    });
}

fn timestamps_pluck(c: &mut Criterion) {
    let ruby = get_ruby_vm();
    let db = generate_timestamp_data(&ruby);
    c.bench_function("pluck timestamps", |b| {
        b.iter(|| db.duck_pluck("SELECT * FROM times".to_owned()))
    });
}

fn timestamps_pluck_to_hash(c: &mut Criterion) {
    let ruby = get_ruby_vm();
    let db = generate_timestamp_data(&ruby);
    c.bench_function("pluck to hash timestamps", |b| {
        b.iter(|| db.duck_pluck_to_hash("SELECT * FROM times".to_owned()))
    });
}

fn timestamps_tz_pluck(c: &mut Criterion) {
    let ruby = get_ruby_vm();
    let db = generate_timestamp_tz_data(&ruby);
    c.bench_function("pluck timestamps with timezones", |b| {
        b.iter(|| db.duck_pluck("SELECT * FROM times".to_owned()))
    });
}

fn timestamps_tz_pluck_to_hash(c: &mut Criterion) {
    let ruby = get_ruby_vm();
    let db = generate_timestamp_tz_data(&ruby);
    c.bench_function("pluck to hash timestamps with timezones", |b| {
        b.iter(|| db.duck_pluck_to_hash("SELECT * FROM times".to_owned()))
    });
}

fn i32_pluck(c: &mut Criterion) {
    let ruby = get_ruby_vm();
    let db = generate_int_data(&ruby);
    c.bench_function("pluck i32", |b| {
        b.iter(|| db.duck_pluck("SELECT * FROM some_ints".to_owned()))
    });
}

fn i32_pluck_to_hash(c: &mut Criterion) {
    let ruby = get_ruby_vm();
    let db = generate_int_data(&ruby);
    c.bench_function("pluck to hash i32", |b| {
        b.iter(|| db.duck_pluck_to_hash("SELECT * FROM some_ints".to_owned()))
    });
}

fn decimal_pluck(c: &mut Criterion) {
    let ruby = get_ruby_vm();
    ruby.require("bigdecimal").unwrap();
    let db = generate_decimal_data(&ruby);
    c.bench_function("pluck decimal", |b| {
        b.iter(|| db.duck_pluck("SELECT * FROM some_decimals".to_owned()))
    });
}

fn decimal_pluck_to_hash(c: &mut Criterion) {
    let ruby = get_ruby_vm();
    ruby.require("bigdecimal").unwrap();
    let db = generate_decimal_data(&ruby);
    c.bench_function("pluck to hash decimal", |b| {
        b.iter(|| db.duck_pluck_to_hash("SELECT * FROM some_decimals".to_owned()))
    });
}

fn small_string_pluck(c: &mut Criterion) {
    let ruby = get_ruby_vm();
    let db = generate_text_data(&ruby, 5);
    c.bench_function("pluck small string", |b| {
        b.iter(|| db.duck_pluck("SELECT * FROM some_texts".to_owned()))
    });
}

fn small_string_pluck_to_hash(c: &mut Criterion) {
    let ruby = get_ruby_vm();
    let db = generate_text_data(&ruby, 5);
    c.bench_function("pluck to hash small string", |b| {
        b.iter(|| db.duck_pluck_to_hash("SELECT * FROM some_texts".to_owned()))
    });
}

fn large_string_pluck(c: &mut Criterion) {
    let ruby = get_ruby_vm();
    let db = generate_text_data(&ruby, 25);
    c.bench_function("pluck large string", |b| {
        b.iter(|| db.duck_pluck("SELECT * FROM some_texts".to_owned()))
    });
}

fn large_string_pluck_to_hash(c: &mut Criterion) {
    let ruby = get_ruby_vm();
    let db = generate_text_data(&ruby, 25);
    c.bench_function("pluck to hash large string", |b| {
        b.iter(|| db.duck_pluck_to_hash("SELECT * FROM some_texts".to_owned()))
    });
}

fn get_ruby_vm() -> Ruby {
    INIT.call_once(|| {
        // is ok at this point, as Once will block other threads + we are not calling same Once
        unsafe {
            CLEANUP = Some(magnus::embed::init());
        }
    });
    Ruby::get().expect("Ruby should have been initialized by now")
}

fn generate_timestamp_data(ruby: &Ruby) -> MutDatabase {
    let options = fake_credentials(&ruby);
    let db = MutDatabase::initialize(options);
    db.execute(r"CREATE TABLE times (time_field TIMESTAMP);".to_owned())
        .unwrap();
    db.execute(r"INSERT INTO times (SELECT CURRENT_TIMESTAMP::TIMESTAMP - INTERVAL (d.days) DAY FROM range(0, 20) AS d(days));".to_owned()).unwrap();
    db
}

fn generate_text_data(ruby: &Ruby, size: usize) -> MutDatabase {
    let options = fake_credentials(&ruby);
    let db = MutDatabase::initialize(options);
    let mut rng = rand::thread_rng();
    let randonm_values = (1..20)
        .map(|_| {
            rng.borrow_mut()
                .sample_iter(&Alphanumeric)
                .take(size)
                .map(char::from)
                .collect::<String>()
        })
        .map(|val| format!("('{}')", val))
        .collect::<Vec<String>>()
        .join(", ");

    db.execute(r"CREATE TABLE some_texts (text_field TEXT);".to_owned())
        .unwrap();
    db.execute(format!(
        "INSERT INTO some_texts(text_field) VALUES {};",
        randonm_values
    ))
    .unwrap();
    db
}

fn generate_int_data(ruby: &Ruby) -> MutDatabase {
    let options = fake_credentials(&ruby);
    let db = MutDatabase::initialize(options);
    let mut rng = rand::thread_rng();
    let randonm_num_values = (1..20)
        .map(|_| format!("({})", rng.gen::<i32>()))
        .collect::<Vec<String>>()
        .join(", ");

    db.execute(r"CREATE TABLE some_ints (int_field INTEGER);".to_owned())
        .unwrap();
    db.execute(format!(
        "INSERT INTO some_ints(int_field) VALUES {};",
        randonm_num_values
    ))
    .unwrap();
    db
}

fn generate_decimal_data(ruby: &Ruby) -> MutDatabase {
    let options = fake_credentials(&ruby);
    let db = MutDatabase::initialize(options);
    let mut rng = rand::thread_rng();
    let randonm_num_values = (1..20)
        .map(|_| format!("({})", rng.gen::<f64>()))
        .collect::<Vec<String>>()
        .join(", ");

    db.execute(r"CREATE TABLE some_decimals (decimal_field DECIMAL);".to_owned())
        .unwrap();
    db.execute(format!(
        "INSERT INTO some_decimals(decimal_field) VALUES {};",
        randonm_num_values
    ))
    .unwrap();
    db
}

fn generate_timestamp_tz_data(ruby: &Ruby) -> MutDatabase {
    let options = fake_credentials(&ruby);
    let db = MutDatabase::initialize(options);
    db.execute(r"CREATE TABLE times (time_tz_field TIMESTAMPTZ);".to_owned())
        .unwrap();
    db.execute(r"INSERT INTO times (SELECT * from generate_series(TIMESTAMP '2024-12-11', TIMESTAMP '2024-12-30', INTERVAL 1 DAY));".to_owned()).unwrap();
    db
}

fn generate_dates_data(ruby: &Ruby) -> MutDatabase {
    let options = fake_credentials(&ruby);
    let db = MutDatabase::initialize(options);
    db.execute(r"CREATE TABLE dates (date_field DATE);".to_owned())
        .unwrap();
    db.execute(r"INSERT INTO dates (SELECT CURRENT_DATE - INTERVAL (d.days) DAY FROM range(0, 20) AS d(days));".to_owned()).unwrap();
    db
}

fn fake_credentials(ruby: &Ruby) -> RHash {
    let options = RHash::new();
    options
        .aset(ruby.str_new("s3_region"), ruby.str_new(""))
        .unwrap();
    options
        .aset(ruby.str_new("s3_access_key_id"), ruby.str_new(""))
        .unwrap();
    options
        .aset(ruby.str_new("s3_secret_access_key"), ruby.str_new(""))
        .unwrap();
    options
}

criterion_group!(
    benches,
    dates_pluck_to_hash,
    dates_pluck,
    timestamps_pluck,
    timestamps_pluck_to_hash,
    timestamps_tz_pluck,
    timestamps_tz_pluck_to_hash,
    // i32_pluck,
    // i32_pluck_to_hash,
    // decimal_pluck,
    // decimal_pluck_to_hash,
    // small_string_pluck,
    // small_string_pluck_to_hash,
    // large_string_pluck,
    // large_string_pluck_to_hash
);
criterion_main!(benches);
