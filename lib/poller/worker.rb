module Poller
  module Worker
    def self.included(base)
      base.extend(ClassMethods)
    end

    module ClassMethods
      def poller_options(options)
        if @poller_options
          @poller_options = @poller_options.merge options
        else
          @poller_options = Poller.default_options.merge options
        end
        Poller.worker_class_registry[options[:queue]] = self if options[:queue]
      end

      def get_poller_options
        @poller_options
      end

      def batch?
        @poller_options[:batch]
      end

      def concurrency
        @poller_options[:concurrency]
      end

      def queue
        @poller_options[:queue]
      end

      def auto_visibility_timeout?
        @poller_options[:auto_visibility_timeout]
      end

      def auto_delete?
        @poller_options[:auto_delete]
      end
    end
  end
end
