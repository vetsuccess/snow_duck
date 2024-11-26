require_relative 'formatter'

module SnowDuck
  module Format
    class MermaidFormatter
      include SnowDuck::Format::Formatter

      def format(database)
        <<-MERMAID
#{config_section}
#{levels_section(database)}
        MERMAID
      end

      private

      # this will group all tables that share same level, under value of that level
      # {
      #  1 => ['table_1', 'table_2'],
      #  2 => ...
      # }
      def resolve_table_levels(vertices)
        level_hash = Hash.new { |hash, key| hash[key] = [] }
        vertex_levels = {}
        vertices.each do |vertex|
          calculate_level(vertex, vertex_levels)
        end
        vertex_levels.each do |vertex, level|
          level_hash[level] << sanitize_node(vertex)
        end
        level_hash
      end

      def calculate_level(vertex, memo = {})
        return memo[vertex.payload] if memo[vertex.payload]

        if vertex.predecessors.empty?
          memo[vertex.payload] = 1
        else
          predecessors_max_level = vertex.predecessors.map { |pred| calculate_level(pred, memo) }.max 
          memo[vertex.payload] = 1 + predecessors_max_level
        end
        memo[vertex.payload]
      end

      def sanitize_node(value)
        relevant_value = value.is_a?(SnowDuck::DDL::EmbeddableData) ? value.table_name : value.to_s
        relevant_value.gsub(/\W/, '_')
      end

      def config_section
<<-CONFIG
---
config:
  layout: elk
  elk:
    mergeEdges: true
    nodePlacementStrategy: LINEAR_SEGMENTS
  theme: dark
---
CONFIG
      end

      def levels_section(database)
        levels = resolve_table_levels(database.dag.vertices)
        mermaid = "graph LR\n"
        levels.each do |level, table_names|
          mermaid += "  subgraph Level#{level}\n"
          table_names.each do |table_name|
            mermaid += "    #{table_name}[#{table_name}]\n"
          end
          mermaid += "  end\n"
        end

        database.table_definitions.each do |table|
          table.depends_on.each do |table_dependency|
            mermaid += "  #{sanitize_node(table_dependency)} --> #{sanitize_node(table)}\n"
          end
        end
        mermaid
      end

    end
  end
end
