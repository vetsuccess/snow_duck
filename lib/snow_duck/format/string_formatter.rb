require_relative 'formatter'

module SnowDuck
  module Format
    class StringFormatter

      include SnowDuck::Format::Formatter

      def format(database)
        output = "Table Dependencies:\n\n"
        database.table_definitions.each do |table|
          if table.depends_on.empty?
            output += "#{table.table_name} has no dependencies.\n"
          else
            dependencies = table.depends_on.map(&:table_name).join(', ')
            output += "#{table.table_name} depends on: #{dependencies}\n"
          end
        end
        output
      end
    end
  end
end