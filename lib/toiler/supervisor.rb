require 'toiler/manager'

module Toiler
  class Supervisor
    attr_accessor :client, :config

    def initialize
      @client = ::Aws::SQS::Client.new
      @config = Celluloid::Supervision::Configuration.new
      define_manager
      define_schedulers
      define_processors
      define_fetchers
      @config.deploy
    end

    def queues
      Toiler.worker_class_registry
    end

    def define_manager
      @config.define type: Manager, as: :manager
    end

    def define_fetchers
      queues.each do |queue, _klass|
        @config.define type: Fetcher, as: "fetcher_#{queue}".to_sym, args: [queue, client]
      end
    end

    def define_schedulers
      queues.each do |queue, _klass|
        @config.define type: Scheduler, as: "scheduler_#{queue}".to_sym, args: []
      end
    end

    def define_processors
      queues.each do |q, klass|
        @config.define type: Celluloid::Supervision::Container::Pool,
                       as: "processor_pool_#{q}".to_sym,
                       args: [actors: Processor, size: klass.concurrency, args: [q]]
      end
    end

    def stop
      terminate_fetchers
      terminate_processors
      Toiler.manager.terminate if Toiler.manager && Toiler.manager.alive?
      @config.shutdown
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
  end
end
