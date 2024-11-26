require_relative 'vertex'

module SnowDuck
  module Graph
    class DAG

      Edge = Struct.new(:origin, :destination, :properties)

      attr_reader :vertices

      #
      # Create a new Directed Acyclic Graph
      #
      # @param [Hash] options configuration options
      # @option options [Module] mix this module into any created +Vertex+
      #
      def initialize(options = {})
        @vertices = []
        @mixin = options[:mixin]
        @n_of_edges = 0
      end

      def add_vertex(payload = {})
        vertex = create_vertex(payload)
        @vertices << vertex
        vertex
      end

      def add_edge(attrs)
        origin = attrs[:origin] || attrs[:source] || attrs[:from] || attrs[:start]
        destination = attrs[:destination] || attrs[:sink] || attrs[:to] || attrs[:end]
        properties = attrs[:properties] || {}

        origin_vertex = @vertices.find { |v| v.payload == origin }
        destination_vertex = @vertices.find { |v| v.payload == destination }
        raise ArgumentError, "Origin #{origin} must be a vertex in this DAG" unless
          my_vertex?(origin_vertex)
        raise ArgumentError, "Destination #{destination} must be a vertex in this DAG" unless
          my_vertex?(destination_vertex)
        raise ArgumentError, "Edge from #{origin} to #{destination} already exists" if
          origin_vertex.successors.include? destination_vertex
        raise ArgumentError, 'A DAG must not have cycles' if origin == destination
        raise ArgumentError, 'A DAG must not have cycles' if
          destination_vertex.path_to?(origin_vertex)
        @n_of_edges += 1
        origin_vertex.send :add_edge, destination_vertex, properties
      end

      # @return Enumerator over all edges in the dag
      def enumerated_edges
        Enumerator.new(@n_of_edges) do |e|
          @vertices.each { |v| v.outgoing_edges.each { |out| e << out } }
        end
      end

      def edges
        enumerated_edges.to_a
      end

      def create_vertex(payload = {})
        Vertex.new(self, payload).tap do |v|
          v.extend(@mixin) if @mixin
        end
      end

      def subgraph(predecessors_of = [], successors_of = [])
        (predecessors_of + successors_of).each do |v|
          raise ArgumentError, 'You must supply a vertex in this DAG' unless
            my_vertex?(v)
        end

        result = self.class.new(mixin: @mixin)
        vertex_mapping = {}

        # Get the set of predecessors verticies and add a copy to the result
        predecessors_set = Set.new(predecessors_of)
        predecessors_of.each { |v| v.ancestors(predecessors_set) }

        predecessors_set.each do |v|
          vertex_mapping[v] = result.add_vertex(v.payload)
        end

        # Get the set of successor vertices and add a copy to the result
        successors_set = Set.new(successors_of)
        successors_of.each { |v| v.descendants(successors_set) }

        successors_set.each do |v|
          vertex_mapping[v] = result.add_vertex(v.payload) unless
            vertex_mapping.include? v
        end

        # get the unique edges
        edge_set = (
          predecessors_set.flat_map(&:incoming_edges) +
            successors_set.flat_map(&:outgoing_edges)
        ).uniq

        # Add them to the result via the vertex mapping
        edge_set.each do |e|
          result.add_edge(
            from: vertex_mapping[e.origin],
            to: vertex_mapping[e.destination],
            properties: e.properties)
        end

        result
      end

      private

      def my_vertex?(v)
        v.is_a?(Vertex) && (v.dag == self)
      end

    end
  end
end