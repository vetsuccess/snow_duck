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

      def initialize_data_for(table_name)
        # make sure we handle all ancestors, recursively
        ancestors = database_definition.table_ancestors(table_name)
        ancestors.each { |ancestor| initialize_data_for(ancestor.table_name) }
        # now we deal with this table
        table_config = database_definition.table_definition_for(table_name)
        if !initialized_tables.include?(table_config.table_name)
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
        pluck_to_hash!("select * from duckdb_tables();")
      end

      def user_defined_views_info
        pluck_to_hash!("select * from duckdb_views;")
      end

      def memory_info
        pluck_to_hash!("select * from duckdb_memory();")
      end

      def drop(object_name, object_type)
        raise ArgumentError, "Unknown object type #{object_type}" unless %i[table view macro function].include?(object_type.to_sym)
        duck_db.execute("DROP #{object_type.to_s} IF EXISTS #{object_name};")
        initialized_tables.delete(object_name)
      end

      def clear_database!
        user_defined_tables_info.each do |table_info|
          snow_duck_logger_object.info("Dropping table #{table_info['table_name']}")
          drop(table_info['table_name'], :table)
        end
        user_defined_views_info.each do |view_info|
          snow_duck_logger_object.info("Dropping view #{view_info['view_name']}")
          drop(view_info['view_name'], :view)
        end
      end

      def dump_database(database_filename)
        database_alias = 'file_dump_database'
        copied_data_names = Set.new
        duck_db.execute_batch("ATTACH '#{database_filename}' AS #{database_alias};")
        # we just want to copy stuff we pulled in, not everything defined
        initialized_tables.each do |initialized_table_name|
          # and again, we need to make sure that ancestors are copied/created first, mostly because of views
          recursively_apply(initialized_table_name) do |config|
            if !copied_data_names.include?(config.table_name)
              duck_db.execute_batch(config.copy_data_ddl(database_alias)) unless copied_data_names.include?(config.table_name)
              copied_data_names.add(config.table_name)
            else
              snow_duck_log("#{config.table_name} already dumped, skipping...")
            end
          end          
        end
        duck_db.execute_batch("DETACH file_dump_database;")
      end

      private

      def recursively_apply(table_name, &block)
        ancestors = database_definition.table_ancestors(table_name)
        ancestors.each do |ancestor|
          recursively_apply(ancestor.table_name, &block)
        end
        table_config = database_definition.table_definition_for(table_name)
        yield table_config if block_given?
      end
  
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