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
        instance_eval(&blk) if block_given?

        # set defaults
        self.client_expiration_secs ||= 3600
      end

    end # Configuration

  end # module Broker
end # module RzmqBrokers
