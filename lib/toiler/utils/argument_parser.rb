module Toiler
  module Utils
    # Parses command-line arguments
    module ArgumentParser
      module_function

      def parse(argv)
        opts = { queues: [] }

        parser = OptionParser.new do |o|
          o.on '-d', '--daemon', 'Daemonize process' do |arg|
            opts[:daemon] = arg
          end

          o.on '-q', '--queue QUEUE1,QUEUE2,...', 'Queues to process' do |arg|
            opts[:active_queues] = arg.split(',')
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

        parser.banner = 'toiler [options]'
        parser.on_tail '-h', '--help', 'Show help' do
          puts parser
          exit 1
        end
        parser.parse!(argv)
        opts
      end
    end
  end
end
