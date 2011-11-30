
class RunBroker
  def initialize(master_context, log_transport)
    @broker_config = RzmqBrokers::Broker::Configuration.new do
      name 'broker-reactor'
      exception_handler nil
      poll_interval 250
      context master_context
      log_endpoint log_transport

      broker_endpoint  "tcp://127.0.0.1:5555"
      broker_bind  true

      broker_klass RzmqBrokers::Majordomo::Broker::Handler
      service_klass RzmqBrokers::Majordomo::Broker::Service
      worker_klass RzmqBrokers::Broker::Worker

      base_msg_klass RzmqBrokers::Majordomo::Messages
    end    
  end
  
  def run
    @broker = RzmqBrokers::Broker::Broker.new(@broker_config) # new thread
  end
end # RunBroker
