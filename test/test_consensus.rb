require 'rubygems'

require File.join("..", "lib", "rzmq-brokers")

Thread.abort_on_exception = true


class Dummy
  Total = 10_000
  def  initialize
    master_context = ZMQ::Context.new
    log_endpoint = 'inproc://reactor_log'
    
    logger_config = ZM::Configuration.new do
      context master_context
      name 'logger-server'
    end

    ZM::Reactor.new(logger_config).run do |reactor|
      log_config = ZM::Server::Configuration.new do
        endpoint log_endpoint
        bind true
        topic ''
        context master_context
        reactor reactor
      end

      log_config.extra = {:file => STDOUT}

      log_server = ZM::LogServer.new(log_config)
    end

    broker_config = RzmqBrokers::Broker::Configuration.new do
      name 'broker-reactor'
      #log_endpoint log_endpoint
      context master_context
      exception_handler nil

      broker_endpoint  "tcp://127.0.0.1:5555"
      broker_bind  true

      broker_klass RzmqBrokers::Consensus::Broker::Handler
      service_klass RzmqBrokers::Consensus::Broker::Service
      worker_klass RzmqBrokers::Broker::Worker
      
      base_msg_klass RzmqBrokers::Majordomo::Messages
    end

    succ, fail = method(:success), method(:failure)
    client_config = RzmqBrokers::Client::Configuration.new do
      name 'client-reactor'
      #log_endpoint log_endpoint
      context master_context
      exception_handler nil

      endpoint  "tcp://127.0.0.1:5555"
      connect  true
      service_name  "clock-lockstep"
      heartbeat_interval 10_000
      heartbeat_retries 3
      on_success  succ
      on_failure  fail
      
      base_msg_klass RzmqBrokers::Majordomo::Messages
    end

    w1, wd = method(:do_work1), method(:worker_disconnect)
    worker1_config = RzmqBrokers::Worker::Configuration.new do
      name 'worker1-reactor'
      #log_endpoint log_endpoint
      context master_context
      exception_handler nil

      endpoint  "tcp://127.0.0.1:5555"
      connect  true
      service_name  "clock-lockstep"
      heartbeat_interval 5_000
      heartbeat_retries 3
      on_request  w1
      on_disconnect wd
      
      base_msg_klass RzmqBrokers::Majordomo::Messages
    end

    w2 = method(:do_work2)
    worker2_config = RzmqBrokers::Worker::Configuration.new do
      name 'worker2-reactor'
      #log_endpoint log_endpoint
      context master_context
      exception_handler nil

      endpoint  "tcp://127.0.0.1:5555"
      connect  true
      service_name  "clock-lockstep"
      heartbeat_interval 8_000
      heartbeat_retries 3
      on_request  w2
      on_disconnect wd
      
      base_msg_klass RzmqBrokers::Majordomo::Messages
    end

    sleep 1
    broker = RzmqBrokers::Broker::Broker.new(broker_config)

    2.times do |i|
      puts "creating worker #{i}"
      worker1 = RzmqBrokers::Worker::Worker.new(worker1_config)
    end

    puts "creating client"
    @client = RzmqBrokers::Client::Client.new(client_config)
  end

  def success(*args)
    #puts "success"
    @times += 1

    if @times < Total
      run
    else
      elapsed = Time.now - @start
      rtt = (elapsed / Total) * 1_000
      puts "DONE!, rtt [#{rtt}] ms per iteration for [#{@times}] iterations in [#{elapsed}] seconds"
      exit
    end
  end
  
  def failure(*args)
    elapsed = Time.now - @start
    puts "failed, processed [#{@times}] times after [#{elapsed}] seconds"
    exit
  end
  
  def do_work1(worker, message)
    worker.succeeded(message.sequence_id, nil)
  end
  
  def do_work2(worker, message)
    worker.succeeded(message.sequence_id, nil)
  end
  
  def worker_disconnect(message)
    puts "worker has disconnected."
  end

  def run
    @client.send_request('clock-lockstep', nil)
  end
  
  def run_first
    @start = Time.now
    @times = 0
    run
  end
end

dummy = Dummy.new
dummy.run_first

sleep
