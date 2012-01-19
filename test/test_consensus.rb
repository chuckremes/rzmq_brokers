require 'rubygems' # for zmqmachine

$:.push(File.expand_path('../lib', File.dirname(__FILE__)))

require "rzmq_brokers"

Thread.abort_on_exception = true


class Dummy
  ClientCount = 300

  def  initialize(port)
    master_context = ZMQ::Context.new
    log_endpoint = 'inproc://reactor_log'
    #broker_endpoint = "inproc://broker_#{port}"
    broker_endpoint = "tcp://127.0.0.1:#{port}"
    interval = nil # uses default when nil

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

    broker_config = RzmqBrokers::Broker::Configuration.new
    broker_config.name ='broker-reactor'
    #broker_config.log_endpoint =log_endpoint
    broker_config.context =master_context
    broker_config.exception_handler= nil
    broker_config.poll_interval= interval

    broker_config.broker_endpoint  = broker_endpoint
    broker_config.broker_bind = true

    broker_config.broker_klass= RzmqBrokers::Consensus::Broker::Handler
    broker_config.service_klass =RzmqBrokers::Consensus::Broker::Service
    broker_config.worker_klass =RzmqBrokers::Broker::Worker

    broker_config.base_msg_klass= RzmqBrokers::Consensus::Messages

    succ, fail = method(:success), method(:failure)
    client_config = RzmqBrokers::Client::Configuration.new
    client_config.name ='client-reactor'
    #client_config.log_endpoint =log_endpoint
    client_config.context =master_context
    client_config.exception_handler= nil
    client_config.poll_interval =interval

    client_config.endpoint  = broker_endpoint
    client_config.connect = true
    client_config.service_name  ="clock-lockstep"
    client_config.heartbeat_interval= 10_000
    client_config.heartbeat_retries= 3
    client_config.on_success  =succ
    client_config.on_failure = fail

    client_config.base_msg_klass= RzmqBrokers::Consensus::Messages

    w1, wd = method(:do_work1), method(:worker_disconnect)
    @worker1_config = RzmqBrokers::Worker::Configuration.new
    @worker1_config.name ='worker1-reactor'
    #@worker1_config.log_endpoint =log_endpoint
    @worker1_config.context= master_context
    @worker1_config.exception_handler= nil
    @worker1_config.poll_interval =interval

    @worker1_config.endpoint  = broker_endpoint
    @worker1_config.connect  =true
    @worker1_config.service_name  ="clock-lockstep"
    @worker1_config.heartbeat_interval= 5_000
    @worker1_config.heartbeat_retries= 3
    @worker1_config.on_request  =w1
    @worker1_config.on_disconnect =wd

    @worker1_config.base_msg_klass =RzmqBrokers::Consensus::Messages

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
    msg = RzmqBrokers::Consensus::Messages::Request.new('clock-lockstep', nil, nil)
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

# JIT warmup
15.times do
  dummy.run_first(1)
  sleep 1 while dummy.running?
end

1.upto(1000) do |i|
  dummy.run_first(i)

  sleep 1 while dummy.running?
end

exit!
