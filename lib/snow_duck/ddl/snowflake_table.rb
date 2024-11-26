require_relative 'table'

module SnowDuck
  module DDL
    class SnowflakeTable < SnowDuck::DDL::Table

      def export_table_data_to_s3
        remote_file = remote_file_location
        query = <<-SQL
            COPY INTO #{remote_file}
            FROM (
                #{ddl_query}
            )
            storage_integration=#{storage_integration}
            FILE_FORMAT = (TYPE = PARQUET)
            HEADER = true
            single=true
            OVERWRITE=true
            max_file_size=4900000000;
        SQL
        connection_provider.connection.execute(query)
        remote_file
      end

      def storage_integration
        "S3_STORAGE_INTEGRATION_#{ENV['SNOWFLAKE_ACCOUNT_NAME']}"
      end

    end
  end
end
