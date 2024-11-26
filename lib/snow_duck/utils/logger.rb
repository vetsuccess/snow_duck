require 'logger'

module SnowDuck
  module Utils
    module Logger

      def snow_duck_log_with_time(message)
        start = Process.clock_gettime(Process::CLOCK_MONOTONIC, :millisecond)
        result = yield
        finish = Process.clock_gettime(Process::CLOCK_MONOTONIC, :millisecond)
        log("#{message} took #{finish - start}ms")
        result
      end

      def snow_duck_log(message)
        snow_duck_logger_object.info(snow_duck_standard_log_message_format(message))
      end

      def snow_duck_log_warn(message)
        snow_duck_logger_object.warn(snow_duck_standard_log_message_format(message))
      end

      def snow_duck_log_error(message)
        snow_duck_logger_object.error(snow_duck_standard_log_message_format(message))
      end

      def snow_duck_standard_log_message_format(message)
        "(#{Time.current.utc}) #{message}"
      end

      def snow_duck_logger_object
        if defined?(Rails)
          Rails.logger
        else
          snow_duck_default_logger
        end
      end

      def snow_duck_default_logger
        @snow_duck_default_logger ||= Logger.new
      end

    end
  end
end