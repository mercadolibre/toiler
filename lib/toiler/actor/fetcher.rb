require 'toiler/actor/utils/actor_logging'
require 'toiler/actor/utils/actor_message'
require 'toiler/aws/queue'

module Toiler
  module Actor
    class Fetcher < Concurrent::Actor::RestartingContext
      include Utils::ActorLogging

      FETCH_LIMIT = 10.freeze

      attr_accessor :queue, :wait, :visibility_timeout, :free_processors

      def initialize(queue, client)
        debug "Initializing Fetcher for queue #{queue}..."
        @queue = Toiler::Aws::Queue.new queue, client
        @wait = Toiler.options[:wait] || 20
        @free_processors = Concurrent::AtomicFixnum.new(0)
        @batch = Toiler.worker_class_registry[queue].batch?
        @visibility_timeout = @queue.visibility_timeout
        debug "Finished initializing Fetcher for queue #{queue}"
      end

      def default_executor
        Concurrent.global_fast_executor
      end

      def on_message(msg)
        case msg.method
        when :poll_messages
          poll_messages(*msg.args)
        when :processor_finished
          processor_finished(*msg.args)
        else
          pass
        end
      rescue
      end

      private

      def batch?
        @batch
      end

      def processor_finished
        debug "Fetcher #{queue.name} received processor finished signal..."
        @free_processors.increment
        reschedule_poll
      end

      def poll_messages
        options = {
          message_attribute_names: %w(All),
          wait_time_seconds: wait
        }

        # AWS limits the batch size by 10
        processors = free_processors.value
        options[:max_number_of_messages] = (batch? || processors > FETCH_LIMIT) ? FETCH_LIMIT : processors
        debug "Fetcher #{queue.name} retreiving messages with options: #{options.inspect}..."
        msgs = queue.receive_messages options
        debug "Fetcher #{queue.name} retreived #{msgs.count} messages..."

        assign_messages msgs unless msgs.empty?
      ensure
        reschedule_poll
      end

      def reschedule_poll
        if free_processors.value > 0
          debug "Fetcher #{queue.name} rescheduling polling due to free_processors being #{free_processors.value}..."
          tell Utils::ActorMessage.new :poll_messages
        end
      end

      def processor_pool
        @processor_pool ||= Toiler.processor_pool queue.name
      end

      def assign_messages(messages)
        debug "Fetcher assigning #{messages.count} for queue #{queue.name}"
        messages = [messages] if batch?
        messages.each do |m|
          processor_pool.tell Utils::ActorMessage.new(:process, [visibility_timeout, m])
          @free_processors.decrement
        end
        debug "Fetcher finished assigning #{messages.count} for queue #{queue.name}"
      end
    end
  end
end
