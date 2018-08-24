# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'toiler/version'

Gem::Specification.new do |spec|
  spec.name          = 'toiler'
  spec.version       = Toiler::VERSION
  spec.authors       = ['Sebastian Schepens']
  spec.email         = ['sebas.schep@hotmail.com']
  spec.description = spec.summary = 'Toiler is a super efficient AWS SQS thread based message processor'
  spec.homepage      = 'https://github.com/sschepens/toiler'
  spec.license       = 'LGPLv3'

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables << 'toiler'
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ['lib']

  spec.add_development_dependency 'rspec'

  spec.add_dependency 'aws-sdk-sqs', '~> 1.0', '>= 1.0.0'
  spec.add_dependency 'concurrent-ruby', '~> 1.0', '>= 1.0.0'
  spec.add_dependency 'concurrent-ruby-edge', '~> 0.3', '>= 0.3'
end
