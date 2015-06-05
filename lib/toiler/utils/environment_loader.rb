require 'erb'
require 'yaml'

module Toiler
  module Utils
    class EnvironmentLoader
      attr_reader :options

      def self.load(options)
        new(options).load
      end

      def self.load_for_rails_console
        load(config_file: (Rails.root + 'config' + 'toiler.yml'))
      end

      def initialize(options)
        @options = options
      end

      def load
        initialize_logger
        load_rails if options[:rails]
        require_workers if options[:require]
        Toiler.options.merge!(config_file_options)
        Toiler.options.merge!(options)
        initialize_aws
      end

      private

      def config_file_options
        if (path = options[:config_file])
          unless File.exist?(path)
            Toiler.logger.warn "Config file #{path} does not exist"
            path = nil
          end
        end

        return {} unless path

        deep_symbolize_keys YAML.load(ERB.new(IO.read(path)).result)
      end

      def initialize_aws
        # aws-sdk tries to load the credentials from the ENV variables: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
        # when not explicit supplied
        fail 'AWS Credentials needed!' if Toiler.options[:aws].empty? && (ENV['AWS_ACCESS_KEY_ID'].nil? || ENV['AWS_SECRET_ACCESS_KEY'].nil?)
        return if Toiler.options[:aws].empty?

        ::Aws.config[:region] = Toiler.options[:aws][:region]
        ::Aws.config[:credentials] = ::Aws::Credentials.new Toiler.options[:aws][:access_key_id], Toiler.options[:aws][:secret_access_key]
      end

      def initialize_logger
        Toiler::Utils::Logging.initialize_logger(options[:logfile])
        Toiler.logger.level = Logger::DEBUG if options[:verbose]
      end

      def load_rails
        # Adapted from: https://github.com/mperham/sidekiq/blob/master/lib/sidekiq/cli.rb

        require 'rails'
        if ::Rails::VERSION::MAJOR < 4
          require File.expand_path('config/environment.rb')
          ::Rails.application.eager_load!
        else
          # Painful contortions, see 1791 for discussion
          require File.expand_path('config/application.rb')
          ::Rails::Application.initializer 'toiler.eager_load' do
            ::Rails.application.config.eager_load = true
          end
          require File.expand_path('config/environment.rb')
        end

        Toiler.logger.info 'Rails environment loaded'
      end

      def require_workers
        require options[:require]
      end

      def deep_symbolize_keys(h)
        h.each_with_object({}) do |(key, value), result|
          result[(key.to_sym rescue key) || key] = value.is_a?(Hash) ? deep_symbolize_keys(value) : value
        end
      end
    end
  end
end
