require_relative '../utils/logger'
require_relative 'embeddable_data'

module SnowDuck
  module DDL
    class Table

      include SnowDuck::DDL::EmbeddableData
      include SnowDuck::Utils::Logger

      attr_reader :ddl_query, :options, :connection_provider, :s3_bucket

      def initialize(options)
        @options = options.with_indifferent_access
        @connection_provider = options.fetch(:connection_provider)
        @ddl_query = generate_ddl
      end

      def copy_data_ddl(database_name)
        "CREATE TABLE #{database_name}.#{table_name} AS SELECT * FROM #{table_name}"
      end

      def define_data(database, database_options)
        if derived_table?
          database.execute_batch("CREATE TABLE #{table_name} AS (#{ddl_query});")
        else
          setup_s3_bucket(database_options)
          snow_duck_log_with_time("Extracting snowflake data for arguments to #{remote_file_location}") { export_table_data_to_s3 }
          snow_duck_log_with_time("Ingesting snowflake data for arguments") do
            if remote_file_type == 'csv'
              # Lets override whatever gets autodetected with explicit types -> this is especially important when file is empty, then it is assumed that every column is VARCHAR
              # https://duckdb.org/docs/stable/data/csv/tips#override-the-types-of-specific-columns
              json_types = column_definitions.to_json
              database.execute_batch("LOAD aws; LOAD httpfs; CREATE TABLE #{table_name} AS SELECT * FROM read_csv('#{remote_file_location}', types = #{json_types});")
            elsif remote_file_type == 'parquet'
              database.execute_batch("LOAD aws; LOAD httpfs; CREATE TABLE #{table_name} AS SELECT * FROM read_parquet('#{remote_file_location}');")
            else
              raise "Unknown format #{remote_file_type}, not sure how to export and ingest it"
            end
          end
          snow_duck_log_with_time("Deleting #{remote_file_location} from S3") { cleanup_remote_files }
        end
      rescue StandardError => e
        raise e unless empty_parquet_file_detected?(e)

        snow_duck_log_warn("Attempted to fetch non existing file: #{remote_file_location}, result set was probably empty")
        columns_def = column_definitions.map { |name, data_type| "#{name} #{data_type}" }.join(', ')
        database.execute_batch("CREATE TABLE #{table_name} (#{columns_def});")
      end

      def setup_s3_bucket(database_options)
        @s3_bucket ||= begin
          s3 = Aws::S3::Resource.new(access_key_id: database_options['s3_access_key_id'], secret_access_key: database_options['s3_secret_access_key'])
          s3.bucket(remote_s3_bucket_name)
        end
      end

      def remote_filename_suffix
        options.key?(:practice_id) ? options[:practice_id] : default_remote_filename_suffix
      end

      def remote_file_path
        # just making it a bit nicer for stuff that works with practice_id
        # (either way, hash of entire options is used to avoid clashes)
        if options.key?(:practice_id)
          "#{ENV['SNOWFLAKE_ACCOUNT_NAME']}/snow_duck_export/#{options[:practice_id]}/#{exported_table_filename}"
        else
          "#{ENV['SNOWFLAKE_ACCOUNT_NAME']}/snow_duck_export/#{exported_table_filename}"
        end
      end

      def remote_file_location
        "s3://#{s3_bucket.name}/#{remote_file_path}"
      end

      def remote_file
        s3_bucket.object(remote_file_path)
      end

      def exported_table_filename
        "#{table_name.gsub('.', '_')}_#{remote_filename_suffix}.#{remote_file_type}"
      end

      # Table definitions are defined with options, like
      #  Lotr::Duck::Tables::Addresses.new(practice_id: 42),
      #  Lotr::Duck::Tables::Addresses.new(practice_id: 75, reference_date: ...)
      # Single database DDL can have multiple tables coming from same table DDL, for example:
      #   tables = [
      #           Lotr::Duck::Tables::Addresses.new(practice_id: 42),
      #           Lotr::Duck::Tables::Addresses.new(practice_id: 75)
      #         ]
      #   db_definition = SnowDuck::DDL::Database.new(tables)
      # but in that case, we need a way of differentiating these tables, so we need a way to override table name, ike so
      #   tables = [
      #     Lotr::Duck::Tables::Addresses.new(practice_id: 42, table_name: 'practice_42_addresses'),
      #     Lotr::Duck::Tables::Addresses.new(practice_id: 75, table_name: Constants::NON_PRACTICE_42_ADDRESSES)
      #   ]
      #   db_definition = SnowDuck::DDL::Database.new(tables)
      def table_name
        options.key?(:table_name) ? options[:table_name] : self.class.table_name
      end

      # If this table depends on other tables, it's initialization will not include export/import of remote data
      # If you have table that should export remote data and it also depends on some other table, split it
      # into tables that are remote-data-only and the one that depends of them
      def derived_table?
        depends_on.any?
      end

      def cleanup_remote_files
        remote_file.delete
      end

      def self.table_name
        raise NotImplementedError
      end

      def generate_ddl
        raise NotImplementedError
      end

      def export_table_data_to_s3
        raise NotImplementedError
      end

      # Used when remote file is missing - can happen when result set is empty
      # In that case we need a way of creating empty DB, without having file to autodetect types and column names
      def column_definitions
        raise NotImplementedError
      end

      def remote_s3_bucket_name
        options[:remote_s3_bucket_name] || ENV['SNOW_DUCK_S3_BUCKET']
      end

      def remote_file_type
        'parquet'.freeze
      end

      # Should be an array of table DDL definition objects
      def depends_on
        []
      end

      def lowercased_column(column_definition, column_name = column_definition)
        "#{column_definition} \"#{column_name.to_s}\""
      end

      def inspect
        "<#TABLE #{table_name}>"
      end

      private

      # Snowflake can't export empty parquet file, with just a header and column definitions
      # We detect it by detecting that we are dealing with 404 error and parquet files
      def empty_parquet_file_detected?(e)
        remote_file_type == 'parquet' && e.message.include?('HTTP Error') && e.message.include?('404 (Not Found)')
      end

      # creates something like '036837ca53b97aa5d0458ac2146b9b557e8751e3b65a09135712f6e476e1af9f' from all values in hash
      def default_remote_filename_suffix
        Digest::SHA2.hexdigest(options.values.map(&:to_s).join)
      end

    end
  end
end
