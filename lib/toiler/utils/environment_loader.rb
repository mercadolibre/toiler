require 'erb'
require 'yaml'

module Toiler
  module Utils
    # Takes care of loading componentes to get toiler ready to run
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

        deep_symbolize_keys YAML.load(ERB.new(File.read(path)).result)
      end

      def initialize_aws
        puts 'Initializing AWS'
        puts Toiler.options
        return if Toiler.options[:aws].empty?
        ::Aws.config[:region] = Toiler.options[:aws][:region]
        ::Aws.config[:endpoint] = Toiler.options[:aws][:endpoint] if Toiler.options[:aws][:endpoint]
        set_aws_credentials
        puts Aws.config[:endpoint]
        #raise 'Finished Initializing'
      end

      def set_aws_credentials
        return unless Toiler.options[:aws][:access_key_id]
        ::Aws.config[:credentials] = ::Aws::Credentials.new(
          Toiler.options[:aws][:access_key_id],
          Toiler.options[:aws][:secret_access_key]
        )
      end

      def initialize_logger
        Toiler::Utils::Logging.initialize_logger(options[:logfile])
        Toiler.logger.level = Logger::DEBUG if options[:verbose]
      end

      def load_rails
        require 'rails'
        if ::Rails::VERSION::MAJOR < 4
          load_rails_old
        else
          load_rails_new
        end
        Toiler.logger.info 'Rails environment loaded'
      end

      def load_rails_old
        require File.expand_path('config/environment.rb')
        ::Rails.application.eager_load!
      end

      def load_rails_new
        require File.expand_path('config/application.rb')
        ::Rails::Application.initializer 'toiler.eager_load' do
          ::Rails.application.config.eager_load = true
        end
        require File.expand_path('config/environment.rb')
      end

      def require_workers
        require options[:require]
      end

      def deep_symbolize_keys(h)
        h.each_with_object({}) do |(key, value), result|
          k = key.respond_to?(:to_sym) ? key.to_sym : key
          result[k] = if value.is_a? Hash
                        deep_symbolize_keys value
                      else
                        value
                      end
        end
      end
    end
  end
end
