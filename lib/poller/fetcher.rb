module Poller
  class Fetcher
    include Celluloid
    include Celluloid::Logger

    FETCH_LIMIT = 10.freeze

    attr_accessor :queue, :delay, :limit

    finalizer :shutdown

    def initialize(queue, client: nil, limit: 1, delay: 20)
      @queue = Queue.new queue, client: client
      @delay = delay
      @limit = limit
      async.poll_messages
    end

    def shutdown
      instance_variables.each { |iv| remove_instance_variable iv }
    end

    def poll_messages
      # AWS limits the batch size by 10
      options = {
        message_attribute_names: %w(All),
        wait_time_seconds: delay
      }

      loop do
        count = Poller.manager.free_processors queue.name
        max = limit < count ? count : limit
        max = max > FETCH_LIMIT ? FETCH_LIMIT : max
        options[:max_number_of_messages] = max
        msgs = queue.receive_messages options
        Poller.manager.assign_messages queue.name, msgs unless msgs.empty?
        Poller.manager.wait_for_available_processors queue.name
      end
    end
  end
end
