module RzmqBrokers
  module Client

    ZMQMachine::ConfigClassMaker.create_class('Configuration', 
    %w( 
    service_name client_id sequence_id on_success on_failure heartbeat_interval timeout_ms heartbeat_retries
    max_broker_timeouts
    base_msg_klass ), 
    ZMQMachine::Server::Configuration, 
    RzmqBrokers::Client)

  end # module Client
end # module RzmqBrokers
