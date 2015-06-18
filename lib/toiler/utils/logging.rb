require 'time'
require 'logger'

module Toiler
  module Utils
    module Logging
      class Pretty < Logger::Formatter
        # Provide a call() method that returns the formatted message.
        def call(severity, time, program_name, message)
          "#{time.utc.iso8601} Pid:#{Process.pid} Actor:#{program_name} Level:#{severity}: #{message.gsub("\n", "\n\t")}\n"
        end
      end

      module_function

      def initialize_logger(log_target = STDOUT)
        log_target = STDOUT if log_target == nil
        @logger = Logger.new(log_target)
        @logger.level = Logger::INFO
        @logger.formatter = Pretty.new
        @logger
      end

      def logger
        @logger || initialize_logger
      end

      def logger=(log)
        @logger = (log ? log : Logger.new('/dev/null'))
      end
    end
  end
end
