module Toiler
  class Fetcher
    include Celluloid
    include Celluloid::Internals::Logger

    FETCH_LIMIT = 10.freeze

    attr_accessor :queue, :wait, :batch, :condition

    finalizer :shutdown

    def initialize(queue, client)
      debug "Initializing Fetcher for queue #{queue}..."
      @condition = Celluloid::Condition.new
      @queue = Queue.new queue, client
      @wait = Toiler.options[:wait] || 20
      @batch = Toiler.worker_class_registry[queue].batch?
      async.poll_messages
      debug "Finished initializing Fetcher for queue #{queue}"
    end

    def shutdown
      debug "Fetcher #{queue.name} shutting down..."
      instance_variables.each { |iv| remove_instance_variable iv }
    end

    def processor_finished
      @condition.broadcast
    end

    def wait_for_available_processors
      manager = Toiler.manager
      @condition.wait if manager.dead? || manager.free_processors(queue) == 0
    end

    def poll_messages
      # AWS limits the batch size by 10
      options = {
        message_attribute_names: %w(All),
        wait_time_seconds: wait
      }

      loop do
        while (count = Toiler.manager.free_processors(queue.name)) == 0
          wait_for_available_processors
        end
        options[:max_number_of_messages] = (batch || count > FETCH_LIMIT) ? FETCH_LIMIT : count
        debug "Fetcher #{queue.name} retreiving messages with options: #{options.inspect}..."
        msgs = queue.receive_messages options
        debug "Fetcher #{queue.name} retreived #{msgs.count} messages..."
        next if msgs.empty?
        Toiler.manager.assign_messages queue.name, msgs
      end
    end
  end
end
