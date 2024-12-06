##
# The DataInitialisation module provides functionality to automatically initialize data
# before executing specific methods. This is particularly useful for ensuring required
# data exists in the database before running tests or specific operations.
#
# @example Basic usage with a test class
#   class MyTest
#     extend SnowDuck::Utils::DataInitialisation
#     
#     # Define required instance variables
#     def initialize
#       @options = { user: 'test_user' }
#       @database = SnowDuck::Data::Database.new
#     end
#
#     # Define the method that needs data initialization
#     def test_user_permissions
#       # Your test code here
#     end
#
#     # Initialize data before the method
#     initialise_data_for :test_user_permissions,
#                        with: [UserData, RoleData],
#                        using_database: :database
#   end
#
# @example Using with custom data definition classes
#   class UserData
#     def initialize(options)
#       @options = options
#     end
#
#     def table_name
#       'users'
#     end
#   end
#
# @note Requirements:
#   - The class must have @options instance variable defined and initialized
#   - The database parameter must be available either as an instance method or instance variable
#   - Each class in the 'with' array must:
#     - Accept options in its constructor
#     - Implement a table_name method
#     - Have a corresponding data initialization in the database
#
module SnowDuck
  module Utils
    module DataInitialisation

      ##
      # Initializes data for a specific method before its execution.
      #
      # @param method_name [Symbol] The name of the method that requires data initialization.
      # @param with [Array<Class>] An array of classes that define the data to be initialized.
      # @param using_database [Symbol] The name of the database instance variable or method.
      #
      # @raise [ArgumentError] If the method_name is not defined in the class.
      # @raise [ArgumentError] If the new method name is already defined.
      # @raise [ArgumentError] If the 'with' array is empty or not an array.
      # @raise [ArgumentError] If the instance variable @options is not defined or initialized.
      # @raise [ArgumentError] If the database instance variable or method is not defined or initialized.
      #
      def initialise_data_for(method_name, with:, using_database:)
        raise ArgumentError, "#{method_name} must be defined in order to initialise data for it" unless method_defined?(method_name.to_sym)

        original_method = instance_method(method_name.to_sym)
        new_method_name = "#{method_name}_with_data_initialised"
        raise ArgumentError, "#{new_method_name} method already defined!" if method_defined?(new_method_name.to_sym)
        raise ArgumentError, "#{with} should be a non-empty array of classes" unless (with.is_a?(Array) && with.size > 0)

        define_method(new_method_name) do |*args, &blk|
          database = _fetch_data_initialisation_variable(using_database)
          options_var = _fetch_data_initialisation_variable('options')

          with.each { |data_definition| database.initialize_data_for(data_definition.new(options_var).table_name) }
          original_method.bind_call(self, *args, &blk)
        end
        alias_method method_name.to_sym, new_method_name.to_sym

        return if method_defined?('_fetch_embedded_database_variable')

        define_method('_fetch_data_initialisation_variable') do |variable_name|
          # if there is a method, we will use that one, if not, let's try to use instance variable
          if self.class.method_defined?(variable_name.to_sym)
            relevant_var = send(variable_name.to_sym)
            raise ArgumentError, "#{variable_name} method returned nil" if relevant_var.nil?
            relevant_var
          else
            raise ArgumentError, "There is neither instance method called #{variable_name} or instance variable @#{variable_name} defined" unless instance_variable_defined?("@#{variable_name}")
            relevant_var = instance_variable_get("@#{variable_name}")
            raise ArgumentError, "Instance variable @#{variable_name} is not initialized at this point" if relevant_var.nil?
            relevant_var
          end
        end
      end

    end
  end
end
