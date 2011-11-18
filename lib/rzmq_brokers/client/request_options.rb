module RzmqBrokers
  module Client

    ZMQMachine::ConfigClassMaker.create_class('RequestOptions',
    %w( timeout_ms retries ),
    Object,
    RzmqBrokers::Client)

    class RequestOptions

      def initialize(&blk)
        instance_eval(&blk) if block_given?

        # set defaults
        self.timeout_ms ||= 0
        self.retries ||= 0
      end

    end # RequestOptions

  end
end
