require 'spec_helper'

require 'toiler/worker'
RSpec.describe Toiler::Worker, type: :model do
  describe "loading class" do
    it "adds the class to the worker registry, under the queue name" do
      class FakeWorker
        include Toiler::Worker
        toiler_options queue: 'test_queue'

        def perform(sqs_message, body); end
      end

      expect(Toiler.worker_class_registry['test_queue']).to eq(FakeWorker)
    end
  end
end
