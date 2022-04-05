require 'json'
require 'toiler/actor/utils/actor_logging'

module Toiler
  module Actor
    # Responsible for processing sqs messages and notifying Fetcher when done
    class Processor < Concurrent::Actor::RestartingContext
      include Utils::ActorLogging

      attr_accessor :queue, :worker_class, :fetcher, :body_parser,
                    :extend_callback, :executing, :thread

      def initialize(queue)
        @queue = queue
        @worker_class = Toiler.worker_class_registry[queue]
        @executing = false
        @thread = nil
        init_options
      end

      def default_executor
        Concurrent.global_io_executor
      end

      def fetcher
        @fetcher ||= Toiler.fetcher queue
      end

      def on_message(msg)
        method, *args = msg
        send(method, *args)
      rescue StandardError, SystemStackError => e
        # rescue SystemStackError, if clients misbehave and cause a stack level too deep exception, we should be able to recover
        error "Processor #{queue} failed processing, reason: #{e.class}: #{e.message}\n#{e.backtrace.join("\n")}"
      end

      def executing?
        @executing
      end

      private

      def init_options
        @deadline_extension = @worker_class.auto_visibility_timeout? || @worker_class.deadline_extension?
        @auto_delete = @worker_class.auto_delete?
        toiler_options = @worker_class.toiler_options
        @body_parser = toiler_options[:parser]
      end

      def deadline_extension?
        @deadline_extension
      end

      def auto_delete?
        @auto_delete
      end

      def process(ack_deadline, msg)
        process_init
        worker = @worker_class.new
        body = get_body(msg)
        timer = deadline_extender ack_deadline, msg, body if deadline_extension?

        debug "Worker #{queue} starts performing..."
        worker.perform msg, body
        debug "Worker #{queue} finishes performing..."
        if msg.is_a? Array
          msg.each(&:delete) if auto_delete?
        else
          msg.delete if auto_delete?
        end
      ensure
        process_cleanup timer
      end

      def process_init
        @executing = true
        @thread = Thread.current
        debug "Processor #{queue} begins processing..."
      end

      def process_cleanup(timer)
        debug "Processor #{queue} starts cleanup after perform..."
        timer.shutdown if timer
        ::ActiveRecord::Base.clear_active_connections! if defined? ActiveRecord
        processor_finished
        @executing = false
        @thread = nil
        debug "Processor #{queue} finished cleanup after perform..."
      end

      def processor_finished
        fetcher.tell :processor_finished
      end

      def deadline_extender(ack_deadline, msg, body)
        interval = [1, ack_deadline / 3].max
        Concurrent::TimerTask.execute execution_interval: interval do |task|
          begin
            msg.modify_ack_deadline! ack_deadline
          rescue StandardError => e
            error "Processor #{queue} failed to extend ack deadline of message - #{e.class}: #{e.message}\n#{e.backtrace.join("\n")}"
            task.shutdown if e.message.include?('ReceiptHandle is invalid')
          end
        end
      end

      def get_body(msg)
        if msg.is_a? Array
          msg.map { |m| parse_body m }
        else
          parse_body msg
        end
      end

      def parse_body(msg)
        case body_parser
        when :json then JSON.parse msg.body
        when Proc then body_parser.call msg
        when :text, nil then msg.body
        else body_parser.load msg.body
        end
      rescue StandardError => e
        raise "Error parsing the message body: #{e.message} - #{msg.body}"
      end
    end
  end
end
