module RzmqBrokers
  module Worker

    ZMQMachine::ConfigClassMaker.create_class('Configuration', 
    %w( service_name on_request on_disconnect heartbeat_interval timeout_ms heartbeat_retries
    base_msg_klass
    ), 
    ZMQMachine::Server::Configuration, 
    RzmqBrokers::Worker)

  end # module Worker
end # module RzmqBrokers
