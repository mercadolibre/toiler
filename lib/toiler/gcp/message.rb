module Toiler
  module Gcp
    # PubSub Message abstraction
    # Provides methods for querying and acting on a PubSub message
    class Message
      attr_accessor :message

      def initialize(message)
        @message = message
      end

      def delete
        message.acknowledge!
      end

      def visibility_timeout=(timeout)
        message.modify_ack_deadline! timeout
      end

      def message_id
        message.message_id
      end

      def body
        message.data
      end

      def attributes
        message.attributes
      end

      def delivery_attempt
        message.delivery_attempt
      end

      def ordering_key
        message.ordering_key
      end

      def reject!
        message.reject!
      end

      def published_at
        message.published_at
      end

      def ack_id
        message.ack_id
      end
    end
  end
end
