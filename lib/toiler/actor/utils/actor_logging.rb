module Toiler
  module Actor
    module Utils
      module ActorLogging
        def error(msg)
          log Logger::Severity::ERROR, msg
        end

        def info(msg)
          log Logger::Severity::INFO, msg
        end

        def debug(msg)
          log Logger::Severity::DEBUG, msg
        end

        def warn(msg)
          log Logger::Severity::WARN, msg
        end

        def fatal(msg)
          log Logger::Severity::FATAL, msg
        end
      end
    end
  end
end
