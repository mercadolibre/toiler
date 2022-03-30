require 'toiler/actor/utils/actor_logging'
require 'toiler/aws/queue'
require 'toiler/gcp/queue'

module Toiler
  module Actor
    # Actor polling for messages only when processors are ready, otherwise idle
    class Fetcher < Concurrent::Actor::RestartingContext
      include Utils::ActorLogging

      FETCH_LIMIT = 10

      attr_accessor :queue, :wait, :visibility_timeout, :free_processors,
                    :executing, :waiting_messages, :concurrency

      def initialize(queue_name, count, provider, provider_config)
        debug "Initializing Fetcher for queue #{queue} for provider #{provider}..."
        if provider.nil? || provider.to_sym == :aws
          @queue = Toiler::Aws::Queue.new queue_name, provider_config
        elsif provider == :gcp
          @queue = Toiler::Gcp::Queue.new queue_name, provider_config
        else
          raise StandardError, "unknown provider #{provider}"
        end
        @wait = Toiler.options[:wait] || 60
        @free_processors = count
        @batch = Toiler.worker_class_registry[queue_name].batch?
        @visibility_timeout = @queue.visibility_timeout
        @executing = false
        @waiting_messages = 0
        @concurrency = count
        debug "Finished initializing Fetcher for queue #{queue_name} for provider #{provider}..."
        tell :poll_messages
      end

      def default_executor
        Concurrent.global_fast_executor
      end

      def on_message(msg)
        @executing = true
        method, *args = msg
        send(method, *args)
      rescue StandardError, SystemStackError => e
        # rescue SystemStackError, if we misbehave and cause a stack level too deep exception, we should be able to recover
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
          queue.receive_messages wait: wait, max_messages: max_number_of_messages
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

        # defer method execution to avoid recursion
        tell :poll_messages if should_poll?
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
