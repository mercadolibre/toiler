require 'toiler/actor/fetcher'
require 'toiler/actor/processor'

module Toiler
  # Starts and supevises Toiler's actors
  class Supervisor
    attr_accessor :client

    def initialize
      @client = ::Aws::SQS::Client.new
      spawn_fetchers
      spawn_processors
    end

    def queues
      Toiler.worker_class_registry
    end

    def spawn_fetchers
      queues.each do |queue, _klass|
        fetcher = Actor::Fetcher.spawn! name: "fetcher_#{queue}".to_sym,
                                        supervise: true, args: [queue, client]
        Toiler.set_fetcher queue, fetcher
      end
    end

    def spawn_processors
      queues.each do |queue, klass|
        name = "processor_pool_#{queue}".to_sym
        count = klass.concurrency
        pool = Concurrent::Actor::Utils::Pool.spawn! name, count do |index|
          Actor::Processor.spawn name: "processor_#{queue}_#{index}".to_sym,
                                 supervise: true, args: [queue]
        end
        Toiler.set_processor_pool queue, pool
      end
    end

    def stop
      terminate_fetchers
      terminate_processors
    end

    def terminate_fetchers
      queues.map do |queue, _klass|
        Toiler.fetcher(queue).ask :terminate!
      end
    end

    def terminate_processors
      queues.each do |queue, _klass|
        Toiler.processor_pool(queue).ask :terminate!
      end
    end
  end
end
