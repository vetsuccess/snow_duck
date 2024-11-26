require_relative 'table'

module SnowDuck
  module DDL
    class PostgresTable < SnowDuck::DDL::Table

      def export_table_data_to_s3
        copy_options = options.fetch(:copy_options, default_copy_options)
        local_data_file_path = "/tmp/#{exported_table_filename}"
        snow_duck_log_with_time("Exporting PG #{table_name} to #{remote_file_location}") { extract_from_postgres(local_data_file_path, copy_options) }
        snow_duck_log_with_time("Uploading PG #{table_name} to #{remote_file_location}") { remote_file.upload_file(local_data_file_path) }
      end

      def remote_file_type
        'csv'
      end

      def extract_from_postgres(file_path, copy_options)
        connection = connection_provider.connection.raw_connection
        File.open(file_path, 'w:UTF-8') do |f|
          connection.copy_data(copy_command(copy_options)) do
            while (line = connection.get_copy_data)
              f.write line.force_encoding(Encoding::UTF_8)
            end
          end
        end
      end

      def copy_command(copy_options)
        "COPY (#{ddl_query}) TO STDOUT WITH (#{copy_options})"
      end

      def default_copy_options
        "FORMAT CSV, HEADER, DELIMITER ','"
      end

    end
  end
end
