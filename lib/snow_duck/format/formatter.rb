module SnowDuck
  module Format
    module Formatter

      def format(_database)
        raise NotImplementedError, "Formatter must implement the format method"
      end
    end
  end
end