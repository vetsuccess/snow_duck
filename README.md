# Snow Duck

Used for providing [DuckDB](https://duckdb.org/) in-memory instance available to Ruby.
Binding to DuckDB is done with Rust language, namely [duckdb-rs](https://github.com/duckdb/duckdb-rs) project.
Then, we use [magnus gem](https://github.com/matsadler/magnus) to build a bridge between rust-land (where duckdb instance 'lives') and ruby-land.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'snow_duck'
```

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install snow_duck

## Usage

TODO: Write usage instructions here

## Development

This gem uses rake-compiler to build a `Makefile` for rust-based native extension, with `create_rust_makefile` method.
You can follow the steps here: https://github.com/matsadler/magnus?tab=readme-ov-file#writing-an-extension-gem-calling-rust-from-ruby

In order to build a shared library that is to be used in gem, run:
`docker compose build ducker && docker compose run --rm ducker bash -c "rake compile"` - this will build docker image with
ruby and rust, and will compile rust extension to a shared object `snow_duck.so`

> [!NOTE]  
> Building this can take a while, mostly because of `libduckdb-sys` crate - it can take up to 15 minutes to compile that one alone

> [!NOTE]  
> For now, only linux-compatible .so file is being built, and only for x86-64 arch