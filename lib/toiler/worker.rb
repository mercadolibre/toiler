module Toiler
  module Worker
    def self.included(base)
      base.extend(ClassMethods)
    end

    module ClassMethods
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
