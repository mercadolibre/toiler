require 'aws-sdk'
require 'toiler/utils/environment_loader'
require 'toiler/utils/logging'
require 'toiler/worker'
require 'toiler/cli'
require 'toiler/version'

module Toiler
  @worker_class_registry = {}
  @options = {
    aws: {}
  }
  @fetchers = {}
  @processor_pools = {}

  attr_reader :worker_class_registry, :options, :fetchers, :processor_pools
  module_function :worker_class_registry, :options, :fetchers, :processor_pools

  module_function

  def logger
    Toiler::Utils::Logging.logger
  end

  def queues
    worker_class_registry.keys
  end

  def fetcher(queue)
    fetchers["fetcher_#{queue}".to_sym]
  end

  def set_fetcher(queue, val)
    fetchers["fetcher_#{queue}".to_sym] = val
  end

  def processor_pool(queue)
    processor_pools["processor_pool_#{queue}".to_sym]
  end

  def set_processor_pool(queue, val)
    processor_pools["processor_pool_#{queue}".to_sym] = val
  end

  def default_options
    {
      auto_visibility_timeout: false,
      concurrency: 1,
      auto_delete: false,
      batch: false
    }
  end
end
