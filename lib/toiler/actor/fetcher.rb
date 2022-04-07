# frozen_string_literal: true

require 'toiler/actor/utils/actor_logging'
require 'toiler/aws/queue'
require 'toiler/gcp/queue'

module Toiler
  module Actor
    # Actor pulling messages only when processors are ready, otherwise idle
    class Fetcher < Concurrent::Actor::RestartingContext
      include Utils::ActorLogging

      attr_reader :queue, :wait, :ack_deadline, :free_processors,
                  :executing, :waiting_messages, :concurrency,
                  :scheduled_task

      def initialize(queue_name, count, provider)
        super()

        debug "Initializing Fetcher for queue #{queue_name} and provider #{provider}..."
        @wait = Toiler.options[:wait] || 60
        @free_processors = count
        @executing = false
        @waiting_messages = 0
        @concurrency = count
        @scheduled_task = nil
        init_queue(queue_name, provider)
        debug "Finished initializing Fetcher for queue #{queue_name} and provider #{provider}..."
        tell :pull_messages
      end

      def default_executor
        Concurrent.global_fast_executor
      end

      def on_message(msg)
        @executing = true
        method, *args = msg
        send(method, *args)
      rescue StandardError, SystemStackError => e
        # if we misbehave and cause a stack level too deep exception, we should be able to recover
        error "Fetcher #{@queue.name} raised exception #{e.class}: #{e.message}\n#{e.backtrace.join("\n")}"
      ensure
        @executing = false
      end

      def executing?
        @executing
      end

      private

      def init_queue(queue_name, provider)
        if provider.nil? || provider.to_sym == :aws
          @queue = Toiler::Aws::Queue.new queue_name, Toiler.aws_client
        elsif provider.to_sym == :gcp
          @queue = Toiler::Gcp::Queue.new queue_name, Toiler.gcp_client
        else
          raise StandardError, "unknown provider #{provider}"
        end
        @ack_deadline = @queue.ack_deadline
      end

      def processor_finished
        debug "Fetcher #{@queue.name} received processor finished signal..."
        @free_processors += 1
        tell :pull_messages
      end

      def pull_future(max_number_of_messages)
        Concurrent::Promises.future do
          @queue.receive_messages wait: @wait, max_messages: max_number_of_messages
        end
      end

      def release_messages(messages)
        @waiting_messages -= messages
      end

      def max_messages
        # limit max messages to 10% of concurrency to always ensure we have
        # 10 concurrent fetches and improved latency
        [@queue.max_messages, (@concurrency * 0.1).ceil].min
      end

      def needed_messages
        @free_processors - @waiting_messages
      end

      def pull_messages
        if needed_messages < max_messages
          # a pull is already scheduled and we dont fit a full batch, return
          return unless @scheduled_task.nil?

          # schedule a message pull if we cannot fill a batch
          # this ensures we wait some time for more messages to arrive
          @scheduled_task = Concurrent::ScheduledTask.execute(0.1) do
            tell [:do_pull_messages, true]
          end
        end

        # we can fit a whole batch, if there was already a scheduled task
        # we just let it run, it will only pull messages if there are more
        # needed messages
        do_pull_messages false
      end

      def do_pull_messages(clear_scheduled_task)
        @scheduled_task = nil if clear_scheduled_task

        return unless should_pull?

        current_needed_messages = needed_messages

        current_needed_messages = max_messages if current_needed_messages >= max_messages

        @waiting_messages += current_needed_messages

        debug "Fetcher #{@queue.name} pulling messages..."
        future = pull_future current_needed_messages
        future.on_rejection! do
          tell [:release_messages, current_needed_messages]
          tell :pull_messages
        end
        future.on_fulfillment! do |msgs|
          tell [:assign_messages, msgs] if !msgs.nil? && !msgs.empty?
          tell [:release_messages, current_needed_messages]
          tell :pull_messages
        end

        # defer method execution to avoid recursion
        tell :pull_messages if should_pull?
      end

      def should_pull?
        needed_messages.positive?
      end

      def processor_pool
        @processor_pool ||= Toiler.processor_pool @queue.name
      end

      def assign_messages(messages)
        messages.each do |m|
          processor_pool.tell [:process, @ack_deadline, m]
          @free_processors -= 1
        end
        debug "Fetcher #{@queue.name} assigned #{messages.count} messages"
      end
    end
  end
end
