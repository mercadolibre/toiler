require 'poller/fetcher'
require 'poller/processor'

module Poller
  class Manager
    include Celluloid
    include Celluloid::Logger

    attr_accessor :queues, :client

    finalizer :shutdown

    def initialize
      Poller.set_manager current_actor
      async.init
    end

    def init
      @queues = Poller.worker_class_registry
      @client = ::Aws::SQS::Client.new
      init_workers
      init_conditions
      pool_processors
      supervise_fetchers
    end

    def shutdown
      instance_variables.each { |iv| remove_instance_variable iv }
    end

    def stop
      terminate_fetchers
      terminate_processors
    end

    def processor_finished(queue)
      @conditions[queue].broadcast
    end

    def init_workers
      Poller.worker_class_registry.each do |q, klass|
        Poller.worker_registry[q] = klass.new
      end
    end

    def supervise_fetchers
      queues.each do |queue, _klass|
        Poller.set_fetcher queue, Fetcher.supervise(queue, client: client).actors.first
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
        Poller.set_processor_pool q, processor
      end
    end

    def terminate_fetchers
      queues.each do |queue, _klass|
        fetcher = Poller.fetcher(queue)
        fetcher.terminate if fetcher && fetcher.alive?
      end
    end

    def terminate_processors
      queues.each do |queue, _klass|
        processor_pool = Poller.processor_pool(queue)
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
      return 1 unless Poller.processor_pool(queue).respond_to? :idle_size
      Poller.processor_pool(queue).idle_size
    end

    def assign_messages(queue, messages)
      processor_pool = Poller.processor_pool(queue)
      if batch? queue
        processor_pool.async.process(queue, messages)
      else
        messages.each do |m|
          processor_pool.async.process(queue, m)
        end
      end
    end

    def wait_for_available_processors(queue)
      @conditions[queue].wait if free_processors(queue) == 0
    end

    def batch?(queue)
      Poller.worker_class_registry[queue].batch?
    end
  end
end
