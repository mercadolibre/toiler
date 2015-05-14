module Toiler
  class Scheduler
    include Celluloid
    include Celluloid::Internals::Logger

    execute_block_on_receiver :custom_every

    def custom_every(*args, block)
      period = args[0]
      block_args = args[1..-1]
      every(period) do
        block.call(*block_args)
      end
    end
  end
end
