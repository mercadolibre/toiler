require 'bundler/setup'
Bundler.setup

require 'concurrent'
require 'concurrent-edge'
Concurrent.use_stdlib_logger Logger::FATAL

require 'toiler'
class TestWorker
  include Toiler::Worker

  toiler_options queue: 'default'

  def perform(sqs_message, body); end
end

require 'rspec'
RSpec.configure do |config|
  config.before do
    Toiler.worker_class_registry = {}
    Toiler.register_worker('default', TestWorker)
  end
end
