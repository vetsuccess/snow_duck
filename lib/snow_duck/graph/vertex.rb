module SnowDuck
  module Graph
    class Vertex
      attr_reader :dag, :payload, :outgoing_edges

      def initialize(dag, payload)
        @dag = dag
        @payload = payload
        @outgoing_edges = []
      end

      private :initialize

      def incoming_edges
        @dag.enumerated_edges.select { |e| e.destination == self }
      end

      def predecessors
        incoming_edges.map(&:origin)
      end

      def successors
        @outgoing_edges.map(&:destination)
      end

      #
      # Is there a path from here to +other+ following edges in the DAG?
      #
      # @param [SnowDuck::Graph::Vertex] other Vertex is the same DAG
      # @raise [ArgumentError] if +other+ is not a Vertex
      # @return true iff there is a path following edges within this DAG
      #
      def path_to?(other)
        raise ArgumentError, 'You must supply a vertex' unless other.is_a? Vertex
        successors.include?(other) || successors.any? { |v| v.path_to? other }
      end

      #
      # Is there a path from +other+ to here following edges in the DAG?
      #
      # @param [SnowDuck::Graph::Vertex] other Vertex is the same DAG
      # @raise [ArgumentError] if +other+ is not a Vertex
      # @return true iff there is a path following edges within this DAG
      #
      def reachable_from?(other)
        raise ArgumentError, 'You must supply a vertex' unless other.is_a? Vertex
        other.path_to? self
      end

      #
      # Retrieve a value from the vertex's payload.
      # This is a shortcut for vertex.payload[key].
      #
      # @param key [Object] the payload key
      # @return the corresponding value from the payload Hash, or nil if not found
      #
      def [](key)
        @payload[key]
      end

      def ancestors(result_set = Set.new)
        predecessors.each do |v|
          unless result_set.include? v
            result_set.add(v)
            v.ancestors(result_set)
          end
        end
        result_set
      end

      def descendants(result_set = Set.new)
        successors.each do |v|
          unless result_set.include? v
            result_set.add(v)
            v.descendants(result_set)
          end
        end
        result_set
      end

      def inspect
        "SnowDuck::Graph::Vertex:#{@payload.inspect}"
      end

      private

      def add_edge(destination, properties)
        SnowDuck::Graph::DAG::Edge.new(self, destination, properties).tap { |e| @outgoing_edges << e }
      end
    end
  end
end