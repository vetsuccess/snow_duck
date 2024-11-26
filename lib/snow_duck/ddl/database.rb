require_relative '../format/string_formatter'
require_relative '../format/mermaid_formatter'
require_relative '../graph/dag'

module SnowDuck
  module DDL
    class Database

      attr_reader :table_definitions, :dag

      def initialize(table_definitions)
        @dag = generate_dag_graph!(table_definitions)
        @table_definitions = table_definitions
      end

      def pretty_print(formatter = SnowDuck::Format::StringFormatter.new)
        raise ArgumentError, "Formatter must implement the 'format' method" unless formatter.respond_to?(:format)

        formatter.format(self)
      end

      def table_definition_for(table_name)
        table_definitions.find { |table_definition| table_definition.table_name == table_name }
      end

      def table_ancestors(table_name)
        current_vertex = dag.vertices.find { |vertex| vertex.payload == table_name }
        raise "Unknown table #{table_name}" if current_vertex.nil?

        current_vertex.ancestors.map { |vertex| table_definition_for(vertex.payload) }
      end

      private

      def generate_dag_graph!(table_definitions)
        validate_table_definitions!(table_definitions)
        graph = populate_graph_with(table_definitions)
        # Add edges based on dependencies
        table_definitions.reduce(graph) { |graph, table_definition| add_table_to_graph(graph, table_definition) }
      end

      def add_table_to_graph(graph, table_definition)
        table_names = graph.vertices.map(&:payload)
        table_definition.depends_on.each do |depends_on_table_ddl|
          unless table_names.include?(depends_on_table_ddl.table_name)
            raise unknown_dependency_error(table_definition, depends_on_table_ddl)
          end

          # Add an edge from the dependency to the current table
          graph.add_edge(from: depends_on_table_ddl.table_name, to: table_definition.table_name)
        end
        graph
      end

      def populate_graph_with(table_definitions, dag = SnowDuck::Graph::DAG.new)
        table_definitions.each do |table_definition|
          # first, we recursively add all dependencies
          dependencies = table_definition.depends_on
          dag = populate_graph_with(dependencies, dag) unless dependencies.empty?
          # and then we add this table as a vertex as well
          dag.add_vertex(table_definition.table_name)
        end
        dag
      end

      # If we detect that there are table definitions with different options but same table name, we error out
      # This means that, potentially, DDL and copy/export statements will differ, but we only have single table name to
      # define
      def validate_table_definitions!(table_definitions)
        raise ArgumentError, 'You must provide at least one table definition' if table_definitions.empty?

        all_definitions = unwind_table_definitions(table_definitions)
        invalid_definitions = all_definitions.select { |_, table_ddls| table_ddls.uniq.size > 1 }
        raise ambiguous_table_name_error(invalid_definitions) if invalid_definitions.any?
      end

      def unwind_table_definitions(table_definitions, result = {})
        table_definitions.each do |table_definition|
          dependencies = table_definition.depends_on
          result = unwind_table_definitions(dependencies, result) unless dependencies.empty?
          result[table_definition.table_name] ||= []
          result[table_definition.table_name] << table_definition.ddl_query
        end
        result
      end

      def unknown_dependency_error(table_definition, table_dependency)
        "Dependency #{table_dependency.table_name} for table #{table_definition.table_name} is not in the tables list."
      end

      def ambiguous_table_name_error(invalid_definitions)
        "Table(s) #{invalid_definitions.keys} have multiple different DDL statements, if you need different tables based on different DDLs, specify custom table name when defining DB"
      end

    end
  end
end
