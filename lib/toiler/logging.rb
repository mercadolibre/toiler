require 'time'
require 'logger'

module Toiler
  module Logging
    class Pretty < Logger::Formatter
      # Provide a call() method that returns the formatted message.
      def call(severity, time, _program_name, message)
        "#{time.utc.iso8601} #{Process.pid} TID-#{Thread.current.object_id.to_s(36)}#{context} #{severity}: #{message}\n"
      end

      def context
        c = Thread.current[:toiler_context]
        c ? " #{c}" : ''
      end
    end

    module_function

    def with_context(msg)
      Thread.current[:toiler_context] = msg
      yield
    ensure
      Thread.current[:toiler_context] = nil
    end

    def initialize_logger(log_target = STDOUT)
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
