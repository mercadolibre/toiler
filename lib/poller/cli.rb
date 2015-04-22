require 'singleton'
require 'optparse'
require 'poller'

module Poller
  # See: https://github.com/mperham/sidekiq/blob/33f5d6b2b6c0dfaab11e5d39688cab7ebadc83ae/lib/sidekiq/cli.rb#L20
  class Shutdown < Interrupt; end

  class CLI
    include Singleton

    def run(args)
      self_read, self_write = IO.pipe

      %w(INT TERM USR1 USR2 TTIN).each do |sig|
        begin
          trap sig do
            self_write.puts(sig)
          end
        rescue ArgumentError
          puts "Signal #{sig} not supported"
        end
      end

      options = parse_cli_args(args)

      EnvironmentLoader.load(options)
      daemonize
      write_pid
      load_celluloid

      begin
        @supervisor = Supervisor.new

        while (readable_io = IO.select([self_read]))
          signal = readable_io.first[0].gets.strip
          handle_signal(signal)
        end
      rescue Interrupt
        @supervisor.stop
        exit 0
      end
    end

    private

    def handle_signal(_signal)
      fail Interrupt
    end

    def load_celluloid
      fail "Celluloid cannot be required until here, or it will break Poller's daemonization" if defined?(::Celluloid) && Poller.options[:daemon]

      # Celluloid can't be loaded until after we've daemonized
      # because it spins up threads and creates locks which get
      # into a very bad state if forked.
      require 'celluloid/autostart'
      Celluloid.logger = (Poller.options[:verbose] ? Poller.logger : nil)
      require 'poller/supervisor'
    end

    def daemonize
      return unless Poller.options[:daemon]

      fail ArgumentError, "You really should set a logfile if you're going to daemonize" unless Poller.options[:logfile]

      files_to_reopen = []
      ObjectSpace.each_object(File) do |file|
        files_to_reopen << file unless file.closed?
      end

      Process.daemon(true, true)

      files_to_reopen.each do |file|
        begin
          file.reopen file.path, 'a+'
          #file.sync = true
        rescue ::Exception
        end
      end

      [$stdout, $stderr].each do |io|
        File.open(Poller.options[:logfile], 'ab') do |f|
          io.reopen(f)
        end
        #io.sync = true
      end
      $stdin.reopen('/dev/null')
    end

    def write_pid
      if (path = Poller.options[:pidfile])
        File.open(path, 'w') do |f|
          f.puts Process.pid
        end
      end
    end

    def parse_cli_args(argv)
      opts = { queues: [] }

      @parser = OptionParser.new do |o|
        o.on '-d', '--daemon', 'Daemonize process' do |arg|
          opts[:daemon] = arg
        end

        o.on '-r', '--require [PATH|DIR]', 'Location of the worker' do |arg|
          opts[:require] = arg
        end

        o.on '-C', '--config PATH', 'Path to YAML config file' do |arg|
          opts[:config_file] = arg
        end

        o.on '-R', '--rails', 'Load Rails' do |arg|
          opts[:rails] = arg
        end

        o.on '-L', '--logfile PATH', 'Path to writable logfile' do |arg|
          opts[:logfile] = arg
        end

        o.on '-P', '--pidfile PATH', 'Path to pidfile' do |arg|
          opts[:pidfile] = arg
        end

        o.on '-v', '--verbose', 'Print more verbose output' do |arg|
          opts[:verbose] = arg
        end
      end

      @parser.banner = 'poller [options]'
      @parser.on_tail '-h', '--help', 'Show help' do
        Poller.logger.info @parser
        exit 1
      end
      @parser.parse!(argv)
      opts
    end
  end
end
