module Toiler
  module Worker
    def self.included(base)
      base.extend(ClassMethods)
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

    module ClassMethods
      def perform_async(body, options = {})
        options ||= {}
        options[:message_attributes] ||= {}
        options[:message_attributes]['toiler_class'] = {
          string_value: self.to_s,
          data_type: 'String'
        }

        options[:message_body] = body

        Toiler::Client.queues(@toiler_options[:queue]).send_message(options)
      end

      def toiler_options(options)
        if @toiler_options
          @toiler_options = @toiler_options.merge options
        else
          @toiler_options = Toiler.default_options.merge options
        end
        Toiler.worker_class_registry[options[:queue]] = self if options[:queue]
      end

      def get_toiler_options
        @toiler_options
      end

      def batch?
        @toiler_options[:batch]
      end

      def concurrency
        @toiler_options[:concurrency]
      end

      def queue
        @toiler_options[:queue]
      end

      def auto_visibility_timeout?
        @toiler_options[:auto_visibility_timeout]
      end

      def auto_delete?
        @toiler_options[:auto_delete]
      end
    end
  end
end
