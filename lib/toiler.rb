require 'aws-sdk'
require 'toiler/core_ext'
require 'toiler/message'
require 'toiler/queue'
require 'toiler/worker'
require 'toiler/environment_loader'
require 'toiler/logging'
require 'toiler/cli'
require 'toiler/version'

module Toiler
  @worker_registry = {}
  @worker_class_registry = {}
  @options = {
    aws: {}
  }

  module_function

  attr_reader :options

  def logger
    Toiler::Logging.logger
  end

  attr_reader :worker_class_registry

  attr_reader :worker_registry

  def queues
    @worker_registry.keys
  end

  def fetcher(queue)
    Celluloid::Actor["fetcher_#{queue}".to_sym]
  end

  def set_fetcher(queue, val)
    Celluloid::Actor["fetcher_#{queue}".to_sym] = val
  end

  def processor_pool(queue)
    Celluloid::Actor["processor_pool_#{queue}".to_sym]
  end

  def set_processor_pool(queue, val)
    Celluloid::Actor["processor_pool_#{queue}".to_sym] = val
  end

  def scheduler(queue)
    Celluloid::Actor["scheduler_#{queue}".to_sym]
  end

  def manager
    Celluloid::Actor[:manager]
  end

  def set_manager(val)
    Celluloid::Actor[:manager] = val
  end

  def timer
    Celluloid::Actor[:timer]
  end

  def set_timer(val)
    Celluloid::Actor[:timer] = val
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
