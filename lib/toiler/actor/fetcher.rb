require 'toiler/actor/utils/actor_logging'
require 'toiler/aws/queue'

module Toiler
  module Actor
    # Actor polling for messages only when processors are ready, otherwise idle
    class Fetcher < Concurrent::Actor::RestartingContext
      include Utils::ActorLogging

      FETCH_LIMIT = 10

      attr_accessor :queue, :wait, :visibility_timeout, :free_processors,
                    :executing, :waiting_messages, :concurrency

      def initialize(queue, client, count)
        debug "Initializing Fetcher for queue #{queue}..."
        @queue = Toiler::Aws::Queue.new queue, client
        @wait = Toiler.options[:wait] || 60
        @free_processors = count
        @batch = Toiler.worker_class_registry[queue].batch?
        @visibility_timeout = @queue.visibility_timeout
        @executing = false
        @waiting_messages = 0
        @concurrency = count
        debug "Finished initializing Fetcher for queue #{queue}"
        tell :poll_messages
      end

      def default_executor
        Concurrent.global_fast_executor
      end

      def on_message(msg)
        @executing = true
        method, *args = msg
        send(method, *args)
      rescue StandardError => e
        error "Fetcher #{queue.name} raised exception #{e.class}: #{e.message}\n#{e.backtrace.join("\n")}"
      ensure
        @executing = false
      end

      def executing?
        @executing
      end

      private

      def batch?
        @batch
      end

      def processor_finished
        debug "Fetcher #{queue.name} received processor finished signal..."
        @free_processors += 1
        tell :poll_messages
      end

      def max_messages
        batch? ? FETCH_LIMIT : [FETCH_LIMIT, free_processors].min
      end

      def poll_future(max_number_of_messages)
        Concurrent::Promises.future do
          queue.receive_messages attribute_names: %w[All],
                                 message_attribute_names: %w[All],
                                 wait_time_seconds: wait,
                                 max_number_of_messages: max_number_of_messages
        end
      end

      def release_messages(messages)
        @waiting_messages -= messages
      end

      def poll_messages
        return unless should_poll?

        max_number_of_messages = max_messages
        return if waiting_messages > 0 && !full_batch?(max_number_of_messages)

        @waiting_messages += max_number_of_messages

        debug "Fetcher #{queue.name} polling messages..."
        future = poll_future max_number_of_messages
        future.on_rejection! do
          tell [:release_messages, max_number_of_messages]
          tell :poll_messages
        end
        future.on_fulfillment! do |msgs|
          tell [:assign_messages, msgs] if !msgs.nil? && !msgs.empty?
          tell [:release_messages, max_number_of_messages]
          tell :poll_messages
        end

        poll_messages if should_poll?
      end

      def should_poll?
        free_processors / 2 > waiting_messages
      end

      def full_batch?(max_number_of_messages)
        max_number_of_messages == FETCH_LIMIT || max_number_of_messages >= concurrency * 0.1
      end

      def processor_pool
        @processor_pool ||= Toiler.processor_pool queue.name
      end

      def assign_messages(messages)
        messages = [messages] if batch?
        messages.each do |m|
          processor_pool.tell [:process, visibility_timeout, m]
          @free_processors -= 1
        end
        debug "Fetcher #{queue.name} assigned #{messages.count} messages"
      end
    end
  end
end
