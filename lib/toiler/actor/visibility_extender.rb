require 'toiler/actor/utils/actor_logging'
require 'timeout'

module Toiler
  module Actor
    # Responsible for processing sqs messages and notifying Fetcher when done
    class VisibilityExtender < Concurrent::Actor::RestartingContext
      include Utils::ActorLogging

      attr_accessor :interval, :visibility_timeout, :extend_callback,
                    :timeout, :timer

      def initialize(visibility_timeout, extend_callback)
        @visibility_timeout = visibility_timeout
        @extend_callback = extend_callback
        @interval = [1, queue_visibility/3].max
        @timeout = [1, queue_visibility-5]
      end

      def default_executor
        Concurrent.global_fast_executor
      end

      def on_message(msg)
        method, *args = msg
        send(method, *args)
      rescue StandardError => e
        error "VisibilityExtender #{queue} failed processing message '#{msg}', reason: #{e.class}\n#{e.backtrace.join("\n")}"
        raise e
      end

      private

      def start(sqs_msg, body)
        @timer = Concurrent::TimerTask.execute execution_interval: interval do
          tell(:extend_visibility, sqs_msg, body, &extend_callback)
        end
      end

      def stop
        timer.shutdown if timer
      end

      def extend_visibility(sqs_msg, body)
        Timeout.timeout(visibility_timeout)
        sqs_msg.visibility_timeout = visibility_timeout
        yield sqs_msg, body if block_given?
        ::ActiveRecord::Base.clear_active_connections! if defined? ActiveRecord
      end
    end
  end
end
