module RzmqBrokers
  module Client

    ZMQMachine::ConfigClassMaker.create_class('Configuration', 
    %w( 
    service_name client_id sequence_id on_success on_failure on_ping heartbeat_interval timeout_ms heartbeat_retries
    max_broker_timeouts
    base_msg_klass ), 
    ZMQMachine::Server::Configuration, 
    RzmqBrokers::Client)

    class Configuration
      def initialize(&blk)
        # set defaults
        self.service_name = ''
        self.client_id = nil
        self.sequence_id = nil
        self.on_success = nil
        self.on_failure = nil
        self.on_ping = nil
        self.heartbeat_interval = 10_000
        self.timeout_ms = 1_000
        self.heartbeat_retries = 3
        self.max_broker_timeouts = 3
        self.base_msg_klass = nil

        instance_eval(&blk) if block_given?
      end
    end # Configuration

  end # module Client
end # module RzmqBrokers
