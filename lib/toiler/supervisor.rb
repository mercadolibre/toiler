require 'toiler/manager'

module Toiler
  class Supervisor < Celluloid::SupervisionGroup
    include Celluloid

    finalizer :shutdown

    def initialize
      @manager = Manager.new
    end

    def stop
      return unless @manager.alive?
      @manager.stop
      @manager.terminate
    end

    def shutdown
      @manager.terminate if @manager.alive?
      instance_variables.each { |iv| remove_instance_variable iv }
    end
  end
end
