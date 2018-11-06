require 'toiler/actor/fetcher'
require 'toiler/actor/processor'

module Toiler
  module Actor
    # Actor that starts and supervises Toiler's actors
    class Supervisor < Concurrent::Actor::RestartingContext
      include Utils::ActorLogging

      attr_accessor :client

      def initialize
        @client = ::Aws::SQS::Client.new
        spawn_processors
        spawn_fetchers
      end

      def on_message(_msg)
        pass
      end

      def spawn_fetchers
        Toiler.active_worker_class_registry.each do |queue, klass|
          count = klass.concurrency
          begin
            fetcher = Actor::Fetcher.spawn! name: "fetcher_#{queue}".to_sym,
                                            supervise: true,
                                            args: [queue, client, count]
            Toiler.set_fetcher queue, fetcher
          rescue StandardError => e
            error "Failed to start Fetcher for queue #{queue}: #{e.message}\n#{e.backtrace.join("\n")}"
          end
        end
      end

      def spawn_processors
        Toiler.active_worker_class_registry.each do |queue, klass|
          name = "processor_pool_#{queue}".to_sym
          count = klass.concurrency
          begin
            pool = Concurrent::Actor::Utils::Pool.spawn! name, count do |index|
              Actor::Processor.spawn name: "processor_#{queue}_#{index}".to_sym,
                                     supervise: true,
                                     args: [queue]
            end
            Toiler.set_processor_pool queue, pool
          rescue StandardError => e
            error "Failed to spawn Processor Pool for queue #{queue}: #{e.message}\n#{e.backtrace.join("\n")}"
          end
        end
      end
    end
  end
end
