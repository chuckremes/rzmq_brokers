
class DBClient
  LoopCount = 50
  
  def initialize(master_context, log_transport)
    success = method(:on_success)
    failure = method(:on_failure)
    
    @client_config = RzmqBrokers::Client::Configuration.new do
      context master_context
      log_endpoint log_transport

      endpoint  "tcp://127.0.0.1:5555"
      connect  true
      max_broker_timeouts 1

      on_success  success
      on_failure  failure

      base_msg_klass RzmqBrokers::Majordomo::Messages
    end
    
    @loops = 0
    @running = false
    @successes = 0
    @failures = 0
  end
  
  def running?() @running; end
  
  def run
    @running = true
    @client = RzmqBrokers::Client::Client.new(@client_config)
    loop_it
  end
  
  def loop_it
    if @loops < LoopCount
      request = LookupRequest.new do
        service_name 'db-lookup'
        
        range_start Time.parse("2011-11-15 17:24:14")
        range_end Time.now
        duration 60 # seconds
        contract_id '123456-jyzf4'
      end
      
      options = RzmqBrokers::Client::RequestOptions.new do
        # adjusting this even a little lower will result in many failures;
        # adjusting higher will result in more successes
        timeout_ms 235
        retries 3
      end
      
      @client.process_request(request, options)
      @loops += 1
    else
      print("All done! Exiting...\n\n")
      @running = false
      exit
    end
  end
  
  def on_success(message)
    @successes += 1
    reply = LookupReplySuccess.from_message(message)
    
    string = "SUCCESS[#{@successes}]: Received a successful reply for request [#{reply.sequence_id.inspect}],\n"
    string += "SUCCESS[#{@successes}]: Results:\n"
    string += "SUCCESS[#{@successes}]: #{reply.answer}"
    string += "\n\n"
    print(string)
    loop_it
  end
  
  def on_failure(message)
    @failures += 1
    reply = LookupReplyFailure.from_message(message)
    string = "FAILURE[#{@failures}]: Received a *failed* reply for request [#{reply.sequence_id.inspect}],\n"
    string += "FAILURE[#{@failures}]: Results:\n"
    string += "FAILURE[#{@failures}]: #{reply.answer}"
    string += "\n\n"
    print(string)
    loop_it
  end
end # class DBClient
