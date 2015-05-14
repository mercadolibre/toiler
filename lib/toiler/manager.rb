require 'toiler/fetcher'
require 'toiler/processor'

module Toiler
  class Manager
    include Celluloid
    include Celluloid::Internals::Logger

    finalizer :shutdown

    def initialize
      async.init
    end

    def init
      debug 'Initializing manager...'
      init_workers
      awake_fetchers
      debug 'Finished initializing manager...'
    end

    def awake_fetchers
      queues.each do |q, _klass|
        fetcher = Toiler.fetcher(q)
        fetcher.processor_finished if fetcher && fetcher.alive? && free_processors(q) > 0
      end
    end

    def queues
      Toiler.worker_class_registry
    end

    def shutdown
      debug 'Manager shutting down...'
      instance_variables.each { |iv| remove_instance_variable iv }
    end

    def processor_finished(queue)
      fetcher = Toiler.fetcher(queue)
      fetcher.processor_finished if fetcher && fetcher.alive?
    end

    def init_workers
      queues.each do |q, klass|
        Toiler.worker_registry[q] = klass.new
      end
    end

    def free_processors(queue)
      pool = Toiler.processor_pool(queue)
      pool && pool.alive? ? pool.idle_size : 0
    end

    def assign_messages(queue, messages)
      debug "Manager assigning #{messages.count} for queue #{queue}"
      processor_pool = Toiler.processor_pool(queue)
      if batch? queue
        processor_pool.async.process(queue, messages)
      else
        messages.each do |m|
          processor_pool.async.process(queue, m)
        end
      end
      debug "Manager finished assigning #{messages.count} for queue #{queue}"
    end

    def batch?(queue)
      queues[queue].batch?
    end
  end
end
