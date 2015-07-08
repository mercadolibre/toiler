module Toiler
  # Toiler's Worker behaviour
  module Worker
    def self.included(base)
      base.extend(ClassMethods)
      base.class_variable_set(:@@toiler_options, Toiler.default_options)
    end

    def log(level, message)
      Toiler.logger.log(level, message, self.class)
    end

    def error(msg)
      log Logger::Severity::ERROR, msg
    end

    def info(msg)
      log Logger::Severity::INFO, msg
    end

    def debug(msg)
      log Logger::Severity::DEBUG, msg
    end

    def warn(msg)
      log Logger::Severity::WARN, msg
    end

    def fatal(msg)
      log Logger::Severity::FATAL, msg
    end

    # Class methods for Workers
    module ClassMethods
      def toiler_options(options = {})
        return class_variable_get(:@@toiler_options) if options.empty?
        Toiler.worker_class_registry[options[:queue]] = self if options[:queue]
        class_variable_get(:@@toiler_options).merge! options
      end

      def batch?
        class_variable_get(:@@toiler_options)[:batch]
      end

      def concurrency
        class_variable_get(:@@toiler_options)[:concurrency]
      end

      def queue
        class_variable_get(:@@toiler_options)[:queue]
      end

      def auto_visibility_timeout?
        class_variable_get(:@@toiler_options)[:auto_visibility_timeout]
      end

      def auto_delete?
        class_variable_get(:@@toiler_options)[:auto_delete]
      end
    end
  end
end
