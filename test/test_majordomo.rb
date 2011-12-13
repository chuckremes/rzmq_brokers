require 'rubygems' # for zmqmachine

$:.push(File.expand_path('../lib', File.dirname(__FILE__)))

require "rzmq_brokers"

Thread.abort_on_exception = true


class Dummy
  Total = 15_000
  WORKERS = 2
  CLIENTS = 10

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
      log_endpoint log_endpoint
      context master_context
      exception_handler nil
      poll_interval 250

      broker_endpoint  "tcp://127.0.0.1:5555"
      broker_bind  true

      broker_klass RzmqBrokers::Majordomo::Broker::Handler
      service_klass RzmqBrokers::Majordomo::Broker::Service
      worker_klass RzmqBrokers::Broker::Worker

      base_msg_klass RzmqBrokers::Majordomo::Messages
    end

    succ, fail = method(:success), method(:failure)
    client_config = RzmqBrokers::Client::Configuration.new do
      name 'client-reactor'
      #log_endpoint log_endpoint
      context master_context
      exception_handler nil
      poll_interval 250

      endpoint  "tcp://127.0.0.1:5555"
      connect  true
      service_name  "db-lookup"
      heartbeat_interval 15_000
      heartbeat_retries 3
      on_success  succ
      on_failure  fail

      base_msg_klass RzmqBrokers::Majordomo::Messages
    end

    w1, wd = method(:do_work1), method(:worker_disconnect)
    worker1_config = RzmqBrokers::Worker::Configuration.new do
      name 'worker1-reactor'
      log_endpoint log_endpoint
      context master_context
      exception_handler nil
      poll_interval 250

      endpoint  "tcp://127.0.0.1:5555"
      connect  true
      service_name  "db-lookup"
      heartbeat_interval 15_000
      heartbeat_retries 3
      on_request  w1
      on_disconnect wd

      base_msg_klass RzmqBrokers::Majordomo::Messages
    end

    w2 = method(:do_work2)
    worker2_config = RzmqBrokers::Worker::Configuration.new do
      name 'worker2-reactor'
      log_endpoint log_endpoint
      context master_context
      exception_handler nil
      poll_interval 250

      endpoint  "tcp://127.0.0.1:5555"
      connect  true
      service_name  "db-lookup"
      heartbeat_interval 13_000
      heartbeat_retries 3
      on_request  w2
      on_disconnect wd

      base_msg_klass RzmqBrokers::Majordomo::Messages
    end

    sleep 1
    broker = RzmqBrokers::Broker::Broker.new(broker_config) # new thread
    @workers = []
    @clients = []
    @reactor = ZM::Reactor.new(ZM::Configuration.create_from(logger_config)).run

    WORKERS.times do |i|
      puts "creating worker #{i}"
      if (i % WORKERS) == 0
        @workers << RzmqBrokers::Worker::Worker.new(worker1_config) # new thread
      elsif (i % WORKERS) > 0
        @workers << RzmqBrokers::Worker::Worker.new(worker2_config) # new thread
      end
    end

    puts "creating clients"
    CLIENTS.times do |i|
      puts "creating client #{i}"
      @clients << RzmqBrokers::Client::Client.new(client_config) # new thread
    end
  end

  def success(*args)
    #puts "success"
    @received += 1

    if @received < Total
      #run
    else
      elapsed = Time.now - @start
      rtt = (elapsed / Total) * 1_000
      puts "DONE!, rtt [#{rtt}] ms per iteration for [#{@received}] iterations in [#{elapsed}] seconds"
      exit!
    end
  end

  def failure(*args)
    elapsed = Time.now - @start
    puts "failed, processed [#{@received}] times after [#{elapsed}] seconds"
    exit
  end

  def do_work1(worker, message)
    @reactor.next_tick do
      worker.succeeded(message.sequence_id, nil)
    end
  end

  def do_work2(worker, message)
    @reactor.next_tick do
      worker.succeeded(message.sequence_id, nil)
    end
  end

  def worker_disconnect(message)
    puts "worker has disconnected."
  end

  def run
    while @sent < Total
      @clients.each do |client|
        @sent += 1
        msg = RzmqBrokers::Majordomo::Messages::ClientRequest.new('db-lookup', nil, nil)
        client.process_request(msg)
      end
    end
    print "\n\nDone sending all requests.\n\n"
  end

  def run_first
    @start = Time.now
    @received = 0
    @sent = 0
    run
  end
end

dummy = Dummy.new
sleep 2 # give everything a chance to spin up before hammering the broker
dummy.run_first

sleep
