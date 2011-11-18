
class DBWorker
  def initialize(master_context, log_transport)
    req_method = method(:do_request)
    dis_method = method(:do_disconnect)
    
    @worker_config = RzmqBrokers::Worker::Configuration.new do
      name 'worker-reactor'
      exception_handler nil
      poll_interval 250
      context master_context
      log_endpoint log_transport

      endpoint  "tcp://127.0.0.1:5555"
      connect  true
      service_name  "db-lookup"
      heartbeat_interval 3_000
      heartbeat_retries 3
      on_request  req_method
      on_disconnect dis_method

      base_msg_klass RzmqBrokers::Majordomo::Messages
    end
  end
  
  def run
    @worker = RzmqBrokers::Worker::Worker.new(@worker_config)
  end
  
  def do_request(worker, message)
    request = DBMessage.create_from(worker, message)
    
    wait = random_wait
    @worker.reactor.log(:debug, "DBWorker will wait [#{wait}] ms before responding.")
    @worker.reactor.oneshot_timer(wait) do
      finish_response(request)
    end
  end
  
  def do_disconnect(message)
    STDERR.puts "how did I get here?"
    exit!
  end
  
  def finish_response(request)
    response = LookupReplySuccess.from_request(request)
    response.answer(find_answer(request))
    @worker.succeeded(response.sequence_id, response.encode)
  end
  
  # Produces a number between 5 and 500; caller interprets
  # the number as milliseconds
  def random_wait
    rand(500) + 5
  end
  
  # Could do a real database lookup, examine a hash, etc.
  #
  def find_answer(request)
    "start -> #{request.range_start}, end -> #{request.range_end}, id -> #{request.contract_id}, status -> succeeded"
  end
end # DBWorker
