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
