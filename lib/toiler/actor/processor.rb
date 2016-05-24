require 'json'
require 'toiler/actor/utils/actor_logging'

module Toiler
  module Actor
    # Responsible for processing sqs messages and notifying Fetcher when done
    class Processor < Concurrent::Actor::RestartingContext
      include Utils::ActorLogging

      attr_accessor :queue, :worker, :fetcher, :body_parser,
                    :extend_callback, :executing, :thread

      def initialize(queue)
        @queue = queue
        @worker = Toiler.worker_class_registry[queue].new
        @fetcher = Toiler.fetcher queue
        @executing = Concurrent::AtomicBoolean.new
        @thread = nil
        init_options
        processor_finished
      end

      def default_executor
        Concurrent.global_io_executor
      end

      def on_message(msg)
        method, *args = msg
        send(method, *args)
      rescue StandardError => e
        error "Processor #{queue} failed processing, reason: #{e.class}\n#{e.backtrace.join("\n")}"
      end

      def executing?
        executing.value
      end

      private

      def init_options
        @auto_visibility_timeout = @worker.class.auto_visibility_timeout?
        @auto_delete = @worker.class.auto_delete?
        toiler_options = @worker.class.toiler_options
        @body_parser = toiler_options[:parser]
        @extend_callback = toiler_options[:on_visibility_extend]
      end

      def auto_visibility_timeout?
        @auto_visibility_timeout
      end

      def auto_delete?
        @auto_delete
      end

      def process(visibility, sqs_msg)
        executing.make_true
        @thread = Thread.current
        debug "Processor #{queue} begins processing..."
        body = get_body(sqs_msg)
        timer = visibility_extender visibility, sqs_msg, body, &extend_callback

        debug "Worker #{queue} starts performing..."
        worker.perform sqs_msg, body
        debug "Worker #{queue} finishes performing..."
        sqs_msg.delete if auto_delete?
      ensure
        process_cleanup timer
        executing.make_false
        @thread = nil
      end

      def process_cleanup(timer)
        debug "Processor #{queue} starts cleanup after perform..."
        timer.shutdown if timer
        ::ActiveRecord::Base.clear_active_connections! if defined? ActiveRecord
        processor_finished
        debug "Processor #{queue} finished cleanup after perform..."
      end

      def processor_finished
        fetcher.tell :processor_finished
      end

      def visibility_extender(queue_visibility, sqs_msg, body)
        return unless auto_visibility_timeout?
        interval = [1,queue_visibility/3].max
        Concurrent::TimerTask.execute execution_interval: interval,
                                      timeout_interval: interval do
          begin
            sqs_msg.visibility_timeout = queue_visibility
            yield sqs_msg, body if block_given?
          rescue StandardError => e
            error "Processor #{queue} failed to extend visibility of message: #{e.message}\n#{e.backtrace.join("\n")}"
          end
        end
      end

      def get_body(sqs_msg)
        if sqs_msg.is_a? Array
          sqs_msg.map { |m| parse_body m }
        else
          parse_body sqs_msg
        end
      end

      def parse_body(sqs_msg)
        case body_parser
        when :json then JSON.parse sqs_msg.body
        when Proc then body_parser.call sqs_msg
        when :text, nil then sqs_msg.body
        else body_parser.load sqs_msg.body
        end
      rescue => e
        raise "Error parsing the message body: #{e.message}"
      end
    end
  end
end
