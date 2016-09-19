require 'spec_helper'

require 'toiler/actor/supervisor'
RSpec.describe Toiler::Actor::Supervisor, type: :model do
  let(:sqs_client) { double(:client) }
  describe "#new" do
    it 'only spawns fetchers for active workers' do
      class InactiveWorker
        include Toiler::Worker
        toiler_options queue: 'inactive_queue'
        def perform(sqs_message, body); end
      end

      Toiler.options.merge!(active_queues: ['default'])
      expect(::Aws::SQS::Client).to receive(:new).and_return(sqs_client)
      expect(Toiler::Actor::Fetcher).to receive(:spawn!).with(name: :fetcher_default, supervise: true, args: ['default', sqs_client])
      expect(Concurrent::Actor::Utils::Pool).to receive(:spawn!).with(:processor_pool_default, 1)
      supervisor = described_class.new
    end

    it 'warns when a queue is missing' do
      Toiler.options.merge!(active_queues: ['missing'])
      expect(Toiler::Utils::Logging.logger).to receive(:warn).with("No worker assigned to queue: missing")
      Toiler.active_worker_class_registry
    end
  end
end
