module SnowDuck
  module DDL
    module EmbeddableData

      # used for bringing data in from remote location
      def define_data(_database)
        raise NotImplementedError
      end

      # used for copying this data to another database
      def copy_data_ddl(_database_name)
        raise NotImplementedError
      end

    end
  end
end
