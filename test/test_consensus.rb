require 'rubygems' # for zmqmachine

$:.push(File.expand_path('../lib', File.dirname(__FILE__)))

require "rzmq_brokers"

Thread.abort_on_exception = true


class Dummy
  ClientCount = 300

  def  initialize(port)
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

      broker_endpoint  "tcp://127.0.0.1:#{port}"
      broker_bind  true

      broker_klass RzmqBrokers::Consensus::Broker::Handler
      service_klass RzmqBrokers::Consensus::Broker::Service
      worker_klass RzmqBrokers::Broker::Worker

      base_msg_klass RzmqBrokers::Consensus::Messages
    end

    succ, fail = method(:success), method(:failure)
    client_config = RzmqBrokers::Client::Configuration.new do
      name 'client-reactor'
      #log_endpoint log_endpoint
      context master_context
      exception_handler nil

      endpoint  "tcp://127.0.0.1:#{port}"
      connect  true
      service_name  "clock-lockstep"
      heartbeat_interval 10_000
      heartbeat_retries 3
      on_success  succ
      on_failure  fail

      base_msg_klass RzmqBrokers::Consensus::Messages
    end

    w1, wd = method(:do_work1), method(:worker_disconnect)
    @worker1_config = RzmqBrokers::Worker::Configuration.new do
      name 'worker1-reactor'
      #log_endpoint log_endpoint
      context master_context
      exception_handler nil

      endpoint  "tcp://127.0.0.1:#{port}"
      connect  true
      service_name  "clock-lockstep"
      heartbeat_interval 5_000
      heartbeat_retries 3
      on_request  w1
      on_disconnect wd

      base_msg_klass RzmqBrokers::Consensus::Messages
    end

    w2 = method(:do_work2)
    worker2_config = RzmqBrokers::Worker::Configuration.new do
      name 'worker2-reactor'
      #log_endpoint log_endpoint
      context master_context
      exception_handler nil

      endpoint  "tcp://127.0.0.1:#{port}"
      connect  true
      service_name  "clock-lockstep"
      heartbeat_interval 8_000
      heartbeat_retries 3
      on_request  w2
      on_disconnect wd

      base_msg_klass RzmqBrokers::Consensus::Messages
    end

    sleep 1
    broker = RzmqBrokers::Broker::Broker.new(broker_config)

    # workers all share the same dedicated reactor thread
    @reactor = ZM::Reactor.new(ZM::Configuration.create_from(logger_config)).run
    @worker1_config.reactor = @reactor
    @workers = []

    puts "creating client"
    @client = RzmqBrokers::Client::Client.new(client_config)
  end

  def setup_clients(count)
    i = 0
    (count - @workers.size).times do
      # dedicated thread per worker
      #      worker1 = RzmqBrokers::Worker::Worker.new(worker1_config)
      @workers << RzmqBrokers::Worker::Handler.new(@worker1_config)
      i += 1
    end
  end

  def success(*args)
    #puts "success"
    @times += 1

    if @times < @total
      run
    else
      elapsed = Time.now - @start
      rtt = (elapsed / @total) * 1_000
      puts "rtt|#{@workers.size}|#{rtt.round}|#{elapsed}"
      @running = false
    end
  end

  def failure(*args)
    elapsed = Time.now - @start
    puts "failed, processed [#{@times}] times after [#{elapsed}] seconds"
    exit!
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
    msg = RzmqBrokers::Consensus::Messages::ClientRequest.new('clock-lockstep', nil, nil)
    @client.process_request(msg)
  end

  def run_first(total_clients)
    @running = true
    @total = 1_000
    setup_clients(total_clients)
    @start = Time.now
    @times = 0
    run
  end

  def running?() @running; end
end

dummy = Dummy.new(ARGV[0])

1.upto(1000) do |i|
  dummy.run_first(i)

  sleep 1 while dummy.running?
end

exit!
