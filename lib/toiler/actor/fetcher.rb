require 'toiler/actor/utils/actor_logging'
require 'toiler/aws/queue'

module Toiler
  module Actor
    # Actor polling for messages only when processors are ready, otherwise idle
    class Fetcher < Concurrent::Actor::RestartingContext
      include Utils::ActorLogging

      FETCH_LIMIT = 10.freeze

      attr_accessor :queue, :wait, :visibility_timeout, :free_processors,
                    :scheduled

      def initialize(queue, client)
        debug "Initializing Fetcher for queue #{queue}..."
        @queue = Toiler::Aws::Queue.new queue, client
        @wait = Toiler.options[:wait] || 20
        @free_processors = Concurrent::AtomicFixnum.new(0)
        @batch = Toiler.worker_class_registry[queue].batch?
        @visibility_timeout = @queue.visibility_timeout
        @scheduled = Concurrent::AtomicBoolean.new
        debug "Finished initializing Fetcher for queue #{queue}"
      end

      def default_executor
        Concurrent.global_fast_executor
      end

      def on_message(msg)
        method, *args = msg
        send(method, *args)
      rescue StandardError => e
        error "Fetcher #{queue.name} raised exception #{e.class}"
      end

      private

      def batch?
        @batch
      end

      def processor_finished
        debug "Fetcher #{queue.name} received processor finished signal..."
        free_processors.increment
        schedule_poll
      end

      def max_messages
        batch? ? FETCH_LIMIT : [FETCH_LIMIT, free_processors.value].min
      end

      def poll_future
        Concurrent.future do
          queue.receive_messages message_attribute_names: %w(All),
                                 wait_time_seconds: wait,
                                 max_number_of_messages: max_messages
        end
      end

      def poll_messages
        poll_future.on_completion! do |_success, msgs, _reason|
          scheduled.make_false
          tell :assign_messages, msgs unless msgs.nil? || msgs.empty?
          schedule_poll
        end
      end

      def schedule_poll
        return unless free_processors.value > 0 && scheduled.make_true
        debug "Fetcher #{queue.name} scheduling polling..."
        tell :poll_messages
      end

      def processor_pool
        @processor_pool ||= Toiler.processor_pool queue.name
      end

      def assign_messages(messages)
        messages = [messages] if batch?
        messages.each do |m|
          processor_pool.tell :process, visibility_timeout, m
          free_processors.decrement
        end
        debug "Fetcher #{queue.name} assigned #{messages.count} messages"
      end
    end
  end
end
