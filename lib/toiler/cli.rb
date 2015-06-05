require 'singleton'
require 'timeout'
require 'optparse'
require 'toiler'

module Toiler
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

      Utils::EnvironmentLoader.load(options)
      daemonize
      write_pid
      load_concurrent

      begin
        require 'toiler/supervisor'
        @supervisor = Supervisor.new

        while (readable_io = IO.select([self_read]))
          signal = readable_io.first[0].gets.strip
          handle_signal(signal)
        end
      rescue Interrupt
        puts 'Received interrupt, terminating actors...'
        begin
          puts 'Waiting up to 60 seconds for actors to finish...'
          Timeout.timeout(60) do
            @supervisor.stop
          end
        ensure
          exit 0
        end
      end
    end

    private

    def handle_signal(_signal)
      fail Interrupt
    end

    def load_concurrent
      fail "Concurrent cannot be required until here, or it will break Toiler's daemonization" if defined?(::Concurrent) && Toiler.options[:daemon]

      # Concurrent can't be loaded until after we've daemonized
      # because it spins up threads and creates locks which get
      # into a very bad state if forked.
      require 'concurrent-edge'
      begin
        require 'concurrent-ext'
      rescue LoadError
      end
      Concurrent.global_logger = lambda do |level, progname, message = nil, &block|
        Toiler.logger.log(level, message, progname, &block)
      end if Toiler.logger
    end

    def daemonize
      return unless Toiler.options[:daemon]

      fail ArgumentError, "You really should set a logfile if you're going to daemonize" unless Toiler.options[:logfile]

      files_to_reopen = []
      ObjectSpace.each_object(File) do |file|
        files_to_reopen << file unless file.closed?
      end

      Process.daemon(true, true)

      files_to_reopen.each do |file|
        begin
          file.reopen file.path, 'a+'
          # file.sync = true
        rescue ::Exception
        end
      end

      [$stdout, $stderr].each do |io|
        File.open(Toiler.options[:logfile], 'ab') do |f|
          io.reopen(f)
        end
        # io.sync = true
      end
      $stdin.reopen('/dev/null')
    end

    def write_pid
      if (path = Toiler.options[:pidfile])
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

      @parser.banner = 'toiler [options]'
      @parser.on_tail '-h', '--help', 'Show help' do
        puts @parser
        exit 1
      end
      @parser.parse!(argv)
      opts
    end
  end
end
