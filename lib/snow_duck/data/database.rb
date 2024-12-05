require_relative '../utils/logger'

module SnowDuck
  module Data
    class Database

      include SnowDuck::Utils::Logger
  
      attr_reader :options, :database_definition, :initialized_tables
  
      def initialize(database_definition, options)
        @options = options.with_indifferent_access
        @database_definition = database_definition
        @initialized_tables = Set.new
      end
  
      def with_tables(*tables)
        Array(tables).each do |table|
          table_name = table.respond_to?(:table_name) ? table.table_name : table.to_s
          initialize_data_for(table_name)
        end
        yield duck_db
      end
  
      def drop_table(table)
        table_name = table.respond_to?(:table_name) ? table.table_name : table.to_s
        duck_db.execute_batch("DROP table #{table_name}")
        initialized_tables.delete(table_name)
      end

      def initialize_data_for(table_name)
        # make sure we handle all ancestors, recursively
        ancestors = database_definition.table_ancestors(table_name)
        ancestors.each { |ancestor| initialize_data_for(ancestor.table_name) }
        # now we deal with this table
        table_config = database_definition.table_definition_for(table_name)
        if initialized_tables.include?(table_config.table_name)
          snow_duck_log("Table #{table_config.table_name} already initialized, skipping remote initialization")
        else
          table_config.define_data(duck_db, s3_credentials)
          initialized_tables.add(table_config.table_name)
        end
      end

      # Can fail if table is not initialized yet, use it only when you know it already is!
      def pluck!(query)
        duck_db.pluck(query)
      end

      # Can fail if table is not initialized yet, use it only when you know it already is!
      def pluck_to_hash!(query)
        duck_db.pluck_to_hash(query)
      end

      def pretty_print(formatter = SnowDuck::Format::MermaidFormatter.new)
        snow_duck_logger_object.info(formatter.format(self))
      end

      def user_defined_tables_info
        pluck_to_hash!("select * from duckdb_tables()")
      end

      def user_defined_views_info
        pluck_to_hash!("select * from duckdb_views")
      end

      def memory_info
        pluck_to_hash!("select * from duckdb_memory()")
      end

      private
  
      def duck_db
        @duck_db ||= begin
          validate_parameters!
          DuckDatabase.new(s3_credentials)
        end
      end

      def s3_credentials
        {
          's3_region' => s3_region,
          's3_access_key_id' => s3_access_key_id,
          's3_secret_access_key' => s3_secret_access_key
        }
      end

  
      def s3_region
        options[:s3_region] || ENV['S3_DUCKDB_REGION']
      end
  
      def s3_access_key_id
        options[:s3_access_key_id] || ENV['S3_DUCKDB_ACCESS_KEY_ID']
      end
  
      def s3_secret_access_key
        options[:s3_secret_access_key] || ENV['S3_DUCKDB_SECRET_ACCESS_KEY']
      end
  
      def validate_parameters!
        raise ArgumentError, missing_argument_message(:s3_region, 'S3_DUCKDB_REGION') if s3_region.blank?
        raise ArgumentError, missing_argument_message(:s3_access_key_id, 'S3_DUCKDB_ACCESS_KEY_ID') if s3_access_key_id.blank?
        raise ArgumentError, missing_argument_message(:s3_secret_access_key, 'S3_DUCKDB_SECRET_ACCESS_KEY') if s3_secret_access_key.blank?
        true
      end
  
      def missing_argument_message(arg_name, arg_env_var_name)
        "Must provide valid #{arg_name}, either in constructor hash or as #{arg_env_var_name} env var"
      end
    end
  end
end