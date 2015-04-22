require 'poller/manager'

module Poller
  class Supervisor < Celluloid::SupervisionGroup
    include Celluloid

    finalizer :shutdown

    def initialize
      @manager = Manager.new
    end

    def stop
      @manager.stop
      @manager.terminate if @manager.alive?
    end

    def shutdown
      @manager.terminate if @manager.alive?
      instance_variables.each { |iv| remove_instance_variable iv }
    end
  end
end
