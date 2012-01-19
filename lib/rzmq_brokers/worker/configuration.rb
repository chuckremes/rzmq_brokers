module RzmqBrokers
  module Worker

    ZMQMachine::ConfigClassMaker.create_class('Configuration', 
    %w( service_name on_request on_disconnect heartbeat_interval timeout_ms heartbeat_retries
    base_msg_klass
    ), 
    ZMQMachine::Server::Configuration, 
    RzmqBrokers::Worker)

    class Configuration
      def initialize(&blk)
        # set defaults
        self.service_name = ''
        self.on_request = nil
        self.on_disconnect = nil
        self.heartbeat_interval = 10_000
        self.timeout_ms = 1_000
        self.heartbeat_retries = 3
        self.base_msg_klass = nil

        instance_eval(&blk) if block_given?
      end
    end # Configuration

  end # module Worker
end # module RzmqBrokers
