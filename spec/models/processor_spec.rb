require 'spec_helper'

require 'toiler/actor/processor'
RSpec.describe Toiler::Actor::Processor, type: :model do
  let(:fetcher) { double(:fetcher) }

  describe "#new" do
    it 'initializes properly' do
      allow(Toiler).to receive(:fetcher).and_return(fetcher)
      processor = described_class.new('default')
      expect(processor.executing?).to eq(false)
    end
  end
end
