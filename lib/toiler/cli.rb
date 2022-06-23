# frozen_string_literal: true

require 'singleton'
require 'timeout'
require 'optparse'
require 'toiler'

module Toiler
  # See: https://github.com/mperham/sidekiq/blob/33f5d6b2b6c0dfaab11e5d39688cab7ebadc83ae/lib/sidekiq/cli.rb#L20
  class Shutdown < Interrupt; end

  # WaitShutdown is used to handle graceful shutdowns
  class WaitShutdown < Interrupt
    attr_accessor :wait

    def initialize(wait)
      super
      @wait = wait
    end
  end

  # Command line client interface
  class CLI
    include Singleton

    attr_accessor :supervisor

    def run(args)
      @self_read, @self_write = IO.pipe

      trap_signals
      options = Utils::ArgumentParser.parse(args)
      Utils::EnvironmentLoader.load(options)
      daemonize
      write_pid
      load_concurrent
      start_supervisor

      handle_stop
    end

    private

    def handle_stop
      # from https://github.com/mperham/sidekiq/blob/6eadad86f9748727ac1e428389f4f67d45103025/lib/sidekiq/cli.rb#L123
      while @self_read.wait_readable
        signal = @self_read.gets.strip
        handle_signal(signal)
      end
    rescue WaitShutdown => e
      Toiler.logger.info "Received Interrupt, Waiting up to #{e.wait} seconds for actors to finish..."
      success = supervisor.ask(:terminate!).wait(e.wait)
      if success
        Toiler.logger.info 'Supervisor successfully terminated'
      else
        Toiler.logger.info 'Timeout waiting for Supervisor to terminate'
      end
    ensure
      exit 0
    end

    def shutdown_pools
      Concurrent.global_fast_executor.shutdown
      Concurrent.global_io_executor.shutdown
      return if Concurrent.global_io_executor.wait_for_termination(60)

      Concurrent.global_io_executor.kill
    end

    def start_supervisor
      require 'toiler/actor/supervisor'
      @supervisor = Actor::Supervisor.spawn! :supervisor
    end

    def trap_signals
      %w[INT TERM QUIT USR1 USR2 TTIN ABRT].each do |sig|
        # from https://github.com/mperham/sidekiq/blob/6eadad86f9748727ac1e428389f4f67d45103025/lib/sidekiq/cli.rb#L54
        old_handler = Signal.trap sig do
          if old_handler.respond_to?(:call)
            begin
              old_handler.call
            rescue Exception => exc
              # signal handlers can't use Logger so puts only
              puts ["Error in #{sig} handler", exc].inspect
            end
          end
          @self_write.puts(sig)
        end
      rescue ArgumentError
        puts "System does not support signal #{sig}"
      end
    end

    def print_stacktraces
      return unless Toiler.logger

      Toiler.logger.info '-------------------'
      Toiler.logger.info 'Received QUIT, dumping threads:'
      Thread.list.each do |t|
        id = t.object_id
        Toiler.logger.info "[thread:#{id}] #{t.backtrace.join("\n[thread:#{id}] ")}"
      end
      Toiler.logger.info '-------------------'
    end

    def print_status
      return unless Toiler.logger

      Toiler.logger.info '-------------------'
      Toiler.logger.info 'Received QUIT, dumping status:'
      Toiler.queues.each do |queue|
        fetcher = Toiler.fetcher(queue).send(:core).send(:context)
        processor_pool = Toiler.processor_pool(queue).send(:core).send(:context)
        processors = processor_pool.instance_variable_get(:@workers).collect { |w| w.send(:core).send(:context) }
        busy_processors = processors.count(&:executing?)
        message = "Status for [queue:#{queue}]:"
        message += "\n[fetcher:#{fetcher.name}] [executing:#{fetcher.executing?}] " \
                   "[waiting_messages:#{fetcher.waiting_messages}] [free_processors:#{fetcher.free_processors}] " \
                   "[scheduled_task:#{!fetcher.scheduled_task.nil?}]"
        message += "\n[processor_pool:#{processor_pool.name}] [workers:#{processors.count}] [busy:#{busy_processors}]"
        processors.each do |processor|
          thread = processor.thread
          thread_id = thread.nil? ? 'nil' : thread.object_id
          message += "\n[processor:#{processor.name}] [executing:#{processor.executing?}] [thread:#{thread_id}]"
          message += " Stack:\n#{thread.backtrace.join("\n\t")}" unless thread.nil?
        end
        Toiler.logger.info message
      end
      Toiler.logger.info '-------------------'
    end

    def handle_signal(signal)
      case signal
      when 'QUIT'
        print_stacktraces
        print_status
      when 'INT', 'TERM'
        raise WaitShutdown, 60
      when 'ABRT'
        raise WaitShutdown, Toiler.options[:shutdown_timeout] * 60
      end
    end

    def load_concurrent
      require 'concurrent-edge'
      if Toiler.logger
        Concurrent.global_logger = lambda do |level, progname, msg = nil, &block|
          Toiler.logger.log(level, msg, progname, &block)
        end
      end
    end

    def daemonize
      return unless Toiler.options[:daemon]
      raise 'Logfile required when daemonizing' unless Toiler.options[:logfile]

      files_to_reopen = []
      ObjectSpace.each_object(File) do |file|
        files_to_reopen << file unless file.closed?
      end

      Process.daemon(true, true)

      reopen_files(files_to_reopen)
      reopen_std
    end

    def reopen_files(files_to_reopen)
      files_to_reopen.each do |file|
        file.reopen file.path, 'a+'
        file.sync = true
      rescue StandardError
        puts "Failed to reopen file #{file}"
      end
    end

    def reopen_std
      [$stdout, $stderr].each do |io|
        File.open(Toiler.options[:logfile], 'ab') do |f|
          io.reopen(f)
        end
        io.sync = true
      end
      $stdin.reopen('/dev/null')
    end

    def write_pid
      file = Toiler.options[:pidfile]
      File.write file, Process.pid if file
    end
  end
end
