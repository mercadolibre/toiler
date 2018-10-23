require 'spec_helper'

require 'toiler/actor/fetcher'
RSpec.describe Toiler::Actor::Fetcher, type: :model do
  let(:queue) { 'default' }
  let(:client) { double(:aws_sqs_client) }

  before do
    allow_any_instance_of(Toiler::Actor::Fetcher).to receive(:log).and_return(true)
    allow_any_instance_of(Toiler::Actor::Fetcher).to receive(:tell)
    allow_any_instance_of(Toiler::Aws::Queue).to receive(:visibility_timeout).and_return(100)
    allow(client).to receive(:get_queue_url).with(queue_name: 'default').and_return double(:queue, queue_url: 'http://aws.fake/queue')
  end

  describe "#new" do
    it 'completes sucessfully' do
      fetcher = described_class.new(queue, client, 1)
      expect(fetcher).to have_received(:tell).with(:poll_messages)
      expect(fetcher.executing?).to eq(false)
    end
  end
end
