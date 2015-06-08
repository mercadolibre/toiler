require 'json'
require 'toiler/actor/utils/actor_logging'
require 'toiler/actor/utils/actor_message'

module Toiler
  module Actor
    class Processor < Concurrent::Actor::RestartingContext
      include Utils::ActorLogging

      attr_accessor :queue, :worker, :fetcher, :body_parser, :on_visibility_extend

      def initialize(balancer, queue)
        super balancer
        @queue = queue
        @worker = Toiler.worker_registry[queue]
        @auto_visibility_timeout = @worker.class.auto_visibility_timeout?
        @auto_delete = @worker.class.auto_delete?
        @body_parser = @worker.class.get_toiler_options[:parser]
        @on_visibility_extend = @worker.class.get_toiler_options[:on_visibility_extend]
        @fetcher = Toiler.fetcher queue
        processor_finished
      end

      def default_executor
        Concurrent.global_io_executor
      end

      def on_message(msg)
        case msg.method
        when :process
          process(*msg.args)
        else
          pass
        end
      rescue StandardError => e
        error "Processor failed processing message, discarding: #{e.message}\n#{e.backtrace.join("\n")}"
      end

      private

      def process(visibility, sqs_msg)
        debug "Processor #{queue} begins processing..."
        timer = auto_visibility_extender(visibility, sqs_msg) if auto_visibility_timeout?

        worker.perform sqs_msg, get_body(sqs_msg)
        sqs_msg.delete if auto_delete?
      rescue StandardError => e
        error "Processor #{queue} faild processing msg: #{e.message}\n#{e.backtrace.join("\n")}"
      ensure
        timer.shutdown if timer
        ::ActiveRecord::Base.clear_active_connections! if defined? ActiveRecord
        processor_finished
        debug "Processor #{queue} finishes processing..."
      end

      def processor_finished
        fetcher.tell Utils::ActorMessage.new :processor_finished
      end

      def auto_visibility_extender(queue_visibility, sqs_msg)
        Concurrent::TimerTask.execute execution_interval: queue_visibility - 5, timeout_interval: queue_visibility - 5 do
          sqs_msg.visibility_timeout = queue_visibility
          on_visibility_extend.call sqs_msg if on_visibility_extend.respond_to? :call
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
        when :json
          JSON.parse sqs_msg.body
        when Proc
          body_parser.call sqs_msg
        when :text, nil
          sqs_msg.body
        else
          body_parser.load sqs_msg.body if body_parser.respond_to? :load # i.e. Oj.load(...) or MultiJson.load(...)
        end
      rescue => e
        error "Error parsing the message body: #{e.message}\nbody_parser: #{body_parser}\nsqs_msg.body: #{sqs_msg.body}"
        nil
      end
    end
  end
end
