##Poller
Poller is a AWS SQS long-polling thread-based message processor.
It's based on [poller](https://github.com/phstc/poller) but takes
a different approach at loadbalancing and uses long-polling.

##Features
###Concurrency
Poller allows to specify the amount of processors (threads) that should be spawned for each queue.
Instead of [poller's](https://github.com/phstc/poller) loadbalancing  approach, Poller delegates this work to the kernel scheduling threads.

###Long-Polling
A Fetcher thread is spawned for each queue.
Fetchers are resposible for polling SQS and retreiving messages.
They are optimised to not bring more messages than the amount of processors avaiable for such queue.
By long-polling fetchers wait for a configurable amount of time for messages to become available on a single request, this prevents unneccesarilly requesting messages when there are none.

###Message Parsing
Workers can configure a parser Class or Proc to parse an SQS message body before being processed.

###Batches
Poller allows a Worker to be able to receive a batch of messages instead of a single one.

##Instalation

Add this line to your application's Gemfile:

```ruby
gem 'poller'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install poller

## Usage

### Worker class

```ruby
class MyWorker
  include Poller::Worker

  poller_options queue: 'default', concurrency: 5, auto_delete: true
  poller_options parser: :json

  # poller_options parser: ->(sqs_msg){ REXML::Document.new(sqs_msg.body) }
  # poller_options parser: MultiJson
  # poller_options auto_visibility_timeout: true
  # poller_options batch: true

  def perform(sqs_msg, body)
    puts body
  end
end
```

### Configuration

```yaml
aws:
  access_key_id:      ...       # or <%= ENV['AWS_ACCESS_KEY_ID'] %>
  secret_access_key:  ...       # or <%= ENV['AWS_SECRET_ACCESS_KEY'] %>
  region:             us-east-1 # or <%= ENV['AWS_REGION'] %>
wait: 20                        # The time in seconds to wait for messages during long-polling
```

### Rails Integration

You can tell Poller to load your Rails application by passing the `-R` or `--rails` flag to the "poller" command.

If you load Rails, and assuming your workers are located in the `app/workers` directory, they will be auto-loaded. This means you don't need to require them explicitly with `-r`.


### Start Poller

```shell
bundle exec poller -r worker.rb -C poller.yml
```

Other options:

```bash
poller --help

    -d, --daemon                     Daemonize process
    -r, --require [PATH|DIR]         Location of the worker
    -C, --config PATH                Path to YAML config file
    -R, --rails                      Load Rails
    -L, --logfile PATH               Path to writable logfile
    -P, --pidfile PATH               Path to pidfile
    -v, --verbose                    Print more verbose output
    -h, --help                       Show help
```


## Credits

Much of the credit goes to [Pablo Cantero](https://github.com/phstc), creator of [Shoryuken](https://github.com/phstc/shoryuken), and [everybody who contributed to it](https://github.com/phstc/shoryuken/graphs/contributors).

## Contributing

1. Fork it ( https://github.com/sschepens/poller/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
