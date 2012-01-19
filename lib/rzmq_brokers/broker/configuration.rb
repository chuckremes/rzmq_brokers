module RzmqBrokers
  module Broker

    ZMQMachine::ConfigClassMaker.create_class('Configuration', 
    %w( 
    broker 
    broker_endpoint broker_bind broker_connect 
    client_expiration_secs 
    broker_klass service_klass worker_klass
    base_msg_klass
    ), 
    ZMQMachine::Server::Configuration, 
    RzmqBrokers::Broker)

    class Configuration
      def initialize(&blk)
        # set defaults
        self.broker = nil
        self.broker_endpoint = nil
        self.broker_bind = false
        self.broker_connect = false
        self.client_expiration_secs = 3600
        self.broker_klass = nil
        self.service_klass = nil
        self.worker_klass = nil
        self.base_msg_klass = nil

        instance_eval(&blk) if block_given?
      end
    end # Configuration

  end # module Broker
end # module RzmqBrokers
