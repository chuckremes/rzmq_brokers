module RzmqBrokers
  module Client

    ZMQMachine::ConfigClassMaker.create_class('RequestOptions',
    %w( timeout_ms retries ),
    Object,
    RzmqBrokers::Client)

    class RequestOptions
      def initialize(&blk)
        # set defaults
        self.timeout_ms = 0
        self.retries = 0

        instance_eval(&blk) if block_given?
      end
    end # RequestOptions

  end
end
