require 'json'
require 'toiler/scheduler'

module Toiler
  class Processor
    include Celluloid
    include Celluloid::Internals::Logger

    attr_accessor :queue

    finalizer :shutdown

    def initialize(queue)
      debug "Initializing Processor for queue #{queue}"
      @queue = queue
      processor_finished
      debug "Finished initializing Processor for queue #{queue}"
    end

    def shutdown
      debug "Processor for queue #{queue} shutting down..."
      ::ActiveRecord::Base.clear_active_connections! if defined? ActiveRecord
      instance_variables.each { |iv| remove_instance_variable iv }
    end

    def process(queue, sqs_msg)
      debug "Processor #{queue} begins processing..."
      worker = Toiler.worker_registry[queue]
      timer = auto_visibility_timeout(queue, sqs_msg, worker.class)

      body = get_body(worker.class, sqs_msg)
      worker.perform(sqs_msg, body)
      sqs_msg.delete if worker.class.auto_delete?
    rescue StandardError => e
      error "Processor #{queue} faild processing msg: #{e.message}\n#{e.backtrace.join("\n")}"
    ensure
      timer.cancel if timer
      ::ActiveRecord::Base.clear_active_connections! if defined? ActiveRecord
      processor_finished
      debug "Processor #{queue} finishes processing..."
    end

    private

    def processor_finished
      Toiler.manager.async.processor_finished queue
    end

    def auto_visibility_timeout(queue, sqs_msg, worker_class)
      return unless worker_class.auto_visibility_timeout?
      queue_visibility_timeout = Toiler.fetcher(queue).queue.visibility_timeout
      block = lambda do |msg, visibility_timeout, q|
        debug "Processor #{q} updating visibility_timeout..."
        msg.visibility_timeout = visibility_timeout
      end

      Toiler.scheduler(queue).custom_every(queue_visibility_timeout - 5, sqs_msg, queue_visibility_timeout, queue, block)
    end

    def get_body(worker_class, sqs_msg)
      if sqs_msg.is_a? Array
        sqs_msg.map { |m| parse_body(worker_class, m) }
      else
        parse_body(worker_class, sqs_msg)
      end
    end

    def parse_body(worker_class, sqs_msg)
      body_parser = worker_class.get_toiler_options[:parser]

      case body_parser
      when :json
        JSON.parse(sqs_msg.body)
      when Proc
        body_parser.call(sqs_msg)
      when :text, nil
        sqs_msg.body
      else
        body_parser.load(sqs_msg.body) if body_parser.respond_to?(:load) # i.e. Oj.load(...) or MultiJson.load(...)
      end
    rescue => e
      logger.error "Error parsing the message body: #{e.message}\nbody_parser: #{body_parser}\nsqs_msg.body: #{sqs_msg.body}"
      nil
    end
  end
end
