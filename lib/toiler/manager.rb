require 'toiler/fetcher'
require 'toiler/processor'

module Toiler
  class Manager
    include Celluloid
    include Celluloid::Logger

    attr_accessor :queues, :client

    finalizer :shutdown

    def initialize
      Toiler.set_manager current_actor
      async.init
    end

    def init
      Toiler.logger.debug 'Initializing manager...'
      @queues = Toiler.worker_class_registry
      @client = ::Aws::SQS::Client.new
      init_workers
      init_conditions
      pool_processors
      supervise_fetchers
      Toiler.logger.debug 'Finished initializing manager...'
    end

    def shutdown
      Toiler.logger.debug 'Manager shutting down...'
      instance_variables.each { |iv| remove_instance_variable iv }
    end

    def stop
      Toiler.logger.debug 'Manager stopping...'
      terminate_fetchers
      terminate_processors
    end

    def processor_finished(queue)
      @conditions[queue].broadcast
    end

    def init_workers
      Toiler.worker_class_registry.each do |q, klass|
        Toiler.worker_registry[q] = klass.new
      end
    end

    def supervise_fetchers
      queues.each do |queue, _klass|
        Toiler.set_fetcher queue, Fetcher.supervise(queue, client).actors.first
      end
    end

    def pool_processors
      queues.each do |q, klass|
        count = klass.concurrency
        processor = if count > 1
                      Processor.pool args: [q], size: count
                    else
                      Processor.supervise(q).actors.first
                    end
        Toiler.set_processor_pool q, processor
      end
    end

    def terminate_fetchers
      queues.each do |queue, _klass|
        fetcher = Toiler.fetcher(queue)
        fetcher.terminate if fetcher && fetcher.alive?
      end
    end

    def terminate_processors
      queues.each do |queue, _klass|
        processor_pool = Toiler.processor_pool(queue)
        processor_pool.terminate if processor_pool && processor_pool.alive?
      end
    end

    def init_conditions
      @conditions = {}
      queues.each do |queue, _klass|
        @conditions[queue] = Celluloid::Condition.new
      end
    end

    def free_processors(queue)
      return 1 unless Toiler.processor_pool(queue).respond_to? :idle_size
      Toiler.processor_pool(queue).idle_size
    end

    def assign_messages(queue, messages)
      Toiler.logger.debug "Manager assigning #{messages.count} for queue #{queue}"
      processor_pool = Toiler.processor_pool(queue)
      if batch? queue
        processor_pool.async.process(queue, messages)
      else
        messages.each do |m|
          processor_pool.async.process(queue, m)
        end
      end
      Toiler.logger.debug "Manager finished assigning #{messages.count} for queue #{queue}"
    end

    def wait_for_available_processors(queue)
      @conditions[queue].wait if free_processors(queue) == 0
    end

    def batch?(queue)
      Toiler.worker_class_registry[queue].batch?
    end
  end
end
