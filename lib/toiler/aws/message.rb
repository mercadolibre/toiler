# frozen_string_literal: true

module Toiler
  module Aws
    # SQS Message abstraction
    # Provides methods for querying and acting on a SQS message
    class Message
      attr_accessor :client, :queue_url, :data

      def initialize(client, queue_url, data)
        @client = client
        @queue_url = queue_url
        @data = data
      end

      def delete
        client.delete_message(
          queue_url: queue_url,
          receipt_handle: data.receipt_handle
        )
      end

      def change_visibility(options)
        client.change_message_visibility(
          options.merge(queue_url: queue_url, receipt_handle: receipt_handle)
        )
      end

      def modify_ack_deadline!(timeout)
        client.change_message_visibility(
          queue_url: queue_url,
          receipt_handle: data.receipt_handle,
          visibility_timeout: timeout
        )
      end

      def message_id
        data.message_id
      end

      def receipt_handle
        data.receipt_handle
      end

      def md5_of_body
        data.md5_of_body
      end

      def body
        data.body
      end

      def attributes
        data.attributes
      end

      def md5_of_message_attributes
        data.md5_of_message_attributes
      end

      def message_attributes
        data.message_attributes
      end
    end
  end
end
