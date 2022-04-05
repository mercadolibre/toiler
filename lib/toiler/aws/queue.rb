require 'toiler/aws/message'

module Toiler
  module Aws
    # SQS Queue abstraction
    # Provides methods for querying and acting on a SQS queue
    class Queue
      attr_accessor :name, :client, :url

      def initialize(name, client)
        @name   = name
        @client = client
        @url    = client.get_queue_url(queue_name: name).queue_url
      end

      def visibility_timeout
        client.get_queue_attributes(
          queue_url: url,
          attribute_names: ['VisibilityTimeout']
        ).attributes['VisibilityTimeout'].to_i
      end

      def delete_messages(options)
        client.delete_message_batch options.merge queue_url: url
      end

      def send_message(options)
        client.send_message sanitize_message_body options.merge queue_url: url
      end

      def send_messages(options)
        client.send_message_batch(
          sanitize_message_body options.merge queue_url: url
        )
      end

      def receive_messages(wait: nil, max_messages: nil)
        client.receive_message(attribute_names: %w[All],
                               message_attribute_names: %w[All],
                               wait_time_seconds: wait,
                               max_number_of_messages: max_messages,
                               queue_url: url)
          .messages
          .map { |m| Message.new(client, url, m) }
      end

      def max_messages
        10
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
