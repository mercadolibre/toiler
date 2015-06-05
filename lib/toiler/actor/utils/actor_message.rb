module Toiler
  module Actor
    module Utils
      ActorMessage = Concurrent::ImmutableStruct.new :method, :args
    end
  end
end
