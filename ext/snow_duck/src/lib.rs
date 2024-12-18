use crate::conversions::string_from_ruby_hash;
use duckdb::{params, Connection, Row};
use magnus::{
    class, define_class, function, method, prelude::*, Error, IntoValue, RArray, RHash,
    StaticSymbol, Value,
};
mod conversions;

pub struct DuckDatabase {
    database: Connection,
}

#[magnus::wrap(class = "DuckDatabase", free_immediately)]
pub struct MutDatabase(std::cell::RefCell<DuckDatabase>);

impl MutDatabase {
    fn row_to_ruby_array(&self, row: &Row<'_>) -> Result<Value, magnus::Error> {
        let column_names = row.as_ref().column_names();
        if column_names.len() > 1 {
            let row_result = RArray::with_capacity(column_names.len());
            for column_name in column_names {
                let current_column_value = row
                    .get::<&str, duckdb::types::Value>(&column_name)
                    .map_err(|err| conversions::to_standard_column_error(&err, &column_name))?;
                let ruby_value = conversions::duck_to_ruby(current_column_value);
                row_result.push(ruby_value)?;
            }
            Ok(row_result.as_value())
        }
        // we are converting single column, do not create array
        else {
            let column_name = column_names.first().ok_or(conversions::to_standard_error(
                "Could not get first column".into(),
            ))?;
            let current_column_value = row
                .get::<&str, duckdb::types::Value>(column_name)
                .map_err(|err| conversions::to_standard_column_error(&err, column_name))?;
            let ruby_value = conversions::duck_to_ruby(current_column_value);
            Ok(ruby_value)
        }
    }

    fn row_to_ruby_hash(&self, row: &Row<'_>, with_indifferent_access_available: bool) -> Result<RHash, magnus::Error> {
        let mut ruby_hash = RHash::new();
        let column_names = row.as_ref().column_names();
        for column_name in column_names {
            let current_column_value = row
                .get::<&str, duckdb::types::Value>(&column_name)
                .map_err(|err| conversions::to_standard_column_error(&err, &column_name))?;
            ruby_hash.aset(
                StaticSymbol::new(column_name),
                conversions::duck_to_ruby(current_column_value),
            )?
        }

        if with_indifferent_access_available {
            ruby_hash = ruby_hash.funcall_public("with_indifferent_access", ())?;
        }

        Ok(ruby_hash)
    }

    pub fn duck_pluck_to_hash(&self, query: String) -> Result<RArray, magnus::Error> {
        let conn = &self.0.borrow().database;
        let mut stmt: duckdb::CachedStatement<'_> = conn
            .prepare_cached(&query)
            .map_err(|err| conversions::to_standard_error(Box::new(err)))?;
        let mut rows = stmt
            .query([])
            .map_err(|err| conversions::to_standard_error(Box::new(err)))?;
        let result = RArray::new();

        let with_indifferent_access_available = RHash::new()
            .respond_to("with_indifferent_access", false)
            .map_err(|err| {
                magnus::Error::new(magnus::exception::standard_error(), err.to_string())
            })?;

        while let Some(row) = rows
            .next()
            .map_err(|err| conversions::to_standard_error(Box::new(err)))?
        {
            let ruby_hash = self.row_to_ruby_hash(row, with_indifferent_access_available)?;
            result.push(ruby_hash).unwrap();
        }
        Ok(result)
    }

    pub fn duck_pluck(&self, query: String) -> Result<RArray, magnus::Error> {
        let conn = &self.0.borrow().database;
        let mut stmt = conn
            .prepare_cached(&query)
            .map_err(|err| conversions::to_standard_error(Box::new(err)))?;
        let mut rows = stmt
            .query([])
            .map_err(|err| conversions::to_standard_error(Box::new(err)))?;
        let result = RArray::new();
        while let Some(row) = rows
            .next()
            .map_err(|err| conversions::to_standard_error(Box::new(err)))?
        {
            let row_result = self.row_to_ruby_array(row)?;
            result.push(row_result)?;
        }
        Ok(result)
    }

    pub fn execute_batch(&self, batch_statement: String) -> Result<magnus::Value, magnus::Error> {
        let database = &self.0.borrow().database;
        database
            .execute_batch(&batch_statement)
            .map(|_| magnus::value::qnil().as_value())
            .map_err(|err| conversions::to_standard_error(Box::new(err)))
    }

    pub fn initialize(options: magnus::RHash) -> Self {
        let database = Connection::open_in_memory().unwrap();
        let s3_region = string_from_ruby_hash(options, "s3_region");
        let s3_access_key_id = string_from_ruby_hash(options, "s3_access_key_id");
        let s3_secret_access_key = string_from_ruby_hash(options, "s3_secret_access_key");

        database
            .execute_batch(
                "INSTALL aws;
                 INSTALL httpfs;",
            )
            .unwrap();
        database
            .execute_batch(&format!(
                "CREATE SECRET aws_bucket_secrets (TYPE S3, KEY_ID '{}', SECRET '{}', REGION '{}')",
                s3_access_key_id, s3_secret_access_key, s3_region
            ))
            .expect("Could not create secrets manager!");
        Self(std::cell::RefCell::from(DuckDatabase { database }))
    }

    pub fn execute(&self, statement: String) -> Result<magnus::Value, magnus::Error> {
        let database = &self.0.borrow().database;
        database
            .execute(&statement, params![])
            .map(|rows_changed| rows_changed.into_value())
            .map_err(|err| conversions::to_standard_error(Box::new(err)))
    }
}

#[magnus::init]
fn init() -> Result<(), Error> {
    let class = define_class("DuckDatabase", class::object())?;
    class.define_singleton_method("new", function!(MutDatabase::initialize, 1))?;
    class.define_method("execute_batch", method!(MutDatabase::execute_batch, 1))?;
    class.define_method("execute", method!(MutDatabase::execute, 1))?;
    class.define_method("pluck", method!(MutDatabase::duck_pluck, 1))?;
    class.define_method("pluck_to_hash", method!(MutDatabase::duck_pluck_to_hash, 1))?;
    Ok(())
}
