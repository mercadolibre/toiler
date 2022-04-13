require 'spec_helper'

require 'toiler/actor/fetcher'
RSpec.describe Toiler::Actor::Fetcher, type: :model do
  let(:queue) { 'default' }
  let(:client) { double(:aws_sqs_client) }
  let(:aws_queue) { double(:aws_queue) }
  let(:gcp_queue) { double(:gcp_queue) }

  before do
    allow_any_instance_of(Toiler::Actor::Fetcher).to receive(:log).and_return(true)
    allow_any_instance_of(Toiler::Actor::Fetcher).to receive(:tell)
    allow(Toiler::Aws::Queue).to receive(:new).and_return(aws_queue)
    allow(Toiler::Gcp::Queue).to receive(:new).and_return(gcp_queue)
    allow(aws_queue).to receive(:ack_deadline).and_return(100)
    allow(gcp_queue).to receive(:ack_deadline).and_return(100)
    allow(client).to receive(:get_queue_url).with(queue_name: 'default').and_return double(:queue, queue_url: 'http://aws.fake/queue')
  end

  describe "#new" do
    context 'default' do
      it 'completes sucessfully' do
        fetcher = described_class.new(queue, 1, nil)
        expect(aws_queue).to have_received(:ack_deadline)
        expect(fetcher).to have_received(:tell).with(:pull_messages)
        expect(fetcher.executing?).to eq(false)
      end
    end

    context 'aws' do
      it 'completes sucessfully' do
        fetcher = described_class.new(queue, 1, :aws)
        expect(aws_queue).to have_received(:ack_deadline)
        expect(fetcher).to have_received(:tell).with(:pull_messages)
        expect(fetcher.executing?).to eq(false)
      end
    end

    context 'gcp' do
      it 'completes sucessfully' do
        fetcher = described_class.new(queue, 1, :gcp)
        expect(gcp_queue).to have_received(:ack_deadline)
        expect(fetcher).to have_received(:tell).with(:pull_messages)
        expect(fetcher.executing?).to eq(false)
      end
    end
  end
end
