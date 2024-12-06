require_relative '../utils/logger'
require_relative 'embeddable_data'

module SnowDuck
  module DDL
    class View

      include SnowDuck::DDL::EmbeddableData
      include SnowDuck::Utils::Logger

      attr_reader :ddl_query, :options

      def initialize(options)
        @options = options.with_indifferent_access
        @ddl_query = generate_ddl
      end

      def define_data(database, _)
        snow_duck_log_with_time("Creating #{table_name} view with options: (#{options})") do
          database.execute_batch("CREATE OR REPLACE VIEW #{table_name} AS (#{generate_ddl})")
        end
      end

      def generate_ddl
        raise NotImplementedError
      end

      def table_name
        options.key?(:table_name) ? options[:table_name] : self.class.table_name
      end

      # Should be an array of table DDL definition objects
      def depends_on
        []
      end

      def lowercased_column(column_definition, column_name = column_definition)
        "#{column_definition} \"#{column_name.to_s}\""
      end

      def inspect
        "<#VIEW #{table_name}>"
      end

    end
  end
end
