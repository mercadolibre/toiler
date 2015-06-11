module Toiler
  module Actor
    module Utils
      module ActorLogging
        def error(msg)
          log Logger::Severity::ERROR, self.class, msg
        end

        def info(msg)
          log Logger::Severity::INFO, self.class, msg
        end

        def debug(msg)
          log Logger::Severity::DEBUG, self.class, msg
        end

        def warn(msg)
          log Logger::Severity::WARN, self.class, msg
        end

        def fatal(msg)
          log Logger::Severity::FATAL, self.class, msg
        end
      end
    end
  end
end
