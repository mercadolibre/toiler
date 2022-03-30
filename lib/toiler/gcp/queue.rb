require 'toiler/gcp/message'

module Toiler
  module Gcp
    # GCP PubSub Queue abstraction
    # Provides methods for querying and acting on a PubSub queue
    class Queue
      attr_accessor :name, :client, :subscription

      def initialize(name, config)
        @name   = name
        @client = ::Google::Cloud::PubSub.new config
        @subscription = client.subscription name
      end

      def visibility_timeout
        subscription.deadline
      end

      def delete_messages(messages)
        subscription.acknowledge(messages)
      end

      def receive_messages(wait: nil, max_messages: nil)
        immediate = wait.nil? || wait == 0
        subscription.pull(immediate: immediate, max: max_messages)
          .messages
          .map { |m| Message.new(m) }
      end

      private

      def sanitize_message_body(options)
        messages = options[:entries] || [options]

        messages.each do |m|
          body = m[:message_body]
          if body.is_a?(Hash)
            m[:message_body] = JSON.dump(body)
          elsif !body.is_a? String
            fail ArgumentError, "Body must be a String, found #{body.class}"
          end
        end

        options
      end
    end
  end
end
