require 'toiler/gcp/message'

module Toiler
  module Gcp
    # GCP PubSub Queue abstraction
    # Provides methods for querying and acting on a PubSub queue
    class Queue
      attr_accessor :name, :subscription

      def initialize(name, client)
        @name   = name
        @subscription = client.subscription name, skip_lookup: true
      end

      def visibility_timeout
        60 # subscription.deadline
      end

      def delete_messages(messages)
        subscription.acknowledge(messages)
      end

      def max_messages
        # pubsub limit is 1000, but it makes little sense to pull so many messages at once
        100
      end

      def receive_messages(wait: nil, max_messages: nil)
        immediate = wait.nil? || wait == 0
        subscription.pull(immediate: immediate, max: max_messages)
          .map { |m| Message.new(m) }
      end
    end
  end
end
