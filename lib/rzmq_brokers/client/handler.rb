module RzmqBrokers
  module Client

    # For use within an existing Reactor
    #
    class Handler
      include ZMQMachine::Server::XREQ

      def initialize(configuration)
        @config = configuration
        @reactor = @config.reactor
        @timeout_ms = @config.timeout_ms
        @sequence_id = @config.sequence_id || 1
        @max_broker_timeouts = @config.max_broker_timeouts
        @broker_timeouts = 0
        @client_id = @config.client_id || generate_client_id
        configure_messages_classes(@config)

        @on_success = @config.on_success
        @on_failure = @config.on_failure

        @requests = Requests.new(@reactor, self)

        server_config = ZM::Server::Configuration.create_from(@config)
        server_config.on_read(method(:on_read))
        super(server_config)
      end

      def on_read(socket, messages, envelope)
        message = @base_msg_klass.create_from(messages, envelope)
        #@reactor.log(:debug, "Client reply, success_reply? [#{message.success_reply?}], failure_reply? [#{message.failure_reply?}], msg.inspect #{message.inspect}")

        if message.success_reply? || message.failure_reply?
          @requests.process_reply(message)
        elsif message.heartbeat?
        end
      end

      def send_request(service_name, sequence_id, payload)
        message = @client_request_msg_klass.new(service_name, sequence_id, payload)
        @reactor.log(:debug, "client, sending request #{message.inspect}")
        write(@base_msg_klass.delimiter + message.to_msgs)
      end

      def process_request(message, request_options = nil)
        request_options ||= RequestOptions.new
        message.sequence_id = get_sequence_id
        @reactor.log(:debug, "client, processing request #{message.inspect}")
        @requests.add(message, request_options)
      end

      def on_success(request, message)
        @on_success.call(message)
      end

      def on_failure(request, message = nil)
        unless message
          @broker_timeouts += 1
          
          if exceeded_broker_timeouts?
            timeouts_exceeded
            return
          else
            message = @client_reply_failure_msg_klass.new(request.service_name, request.sequence_id, nil)
          end
        end
        
        @on_failure.call(message)
      end

      # Called when the number of timeout-related failures has exceeded the allowable
      # number. The Broker is considered to be dead, so we close the old and reopen
      # a new socket.
      #
      # This failure *may occur* due to the Broker dropping our requests due to a
      # client_id collision with another client (it's only a 32-bit space). So, we
      # also regenerate our client ID before reconnecting.
      #
      def timeouts_exceeded
        @reactor.log(:warn, "Client exceeded allowable [#{@max_broker_timeouts}] timeout failures; reopening socket to Broker!")

        # active requests that haven't timed out & failed will still have the old
        # client ID; we need to restart those requests with the new ID
        @broker_timeouts = 0
        reopen_socket
        @client_id = generate_client_id
        @requests.restart_all_with_client_id(@client_id)
      end


      private

      # Returns the current sequence number and advances it by 1 for
      # the next call. It's coupled with the client_id for convenience so
      # this returns a unique identifier for each request.
      #
      def get_sequence_id
        number = @sequence_id
        @sequence_id += 1
        [number, @client_id]
      end

      def configure_messages_classes(config)
        @base_msg_klass = config.base_msg_klass.const_get('Message')
        parent = config.base_msg_klass
        @client_request_msg_klass = parent.const_get('ClientRequest')
        @client_reply_success_msg_klass = parent.const_get('ClientReplySuccess')
        @client_reply_failure_msg_klass = parent.const_get('ClientReplyFailure')
      end

      def generate_client_id
        FNV.hash_string(UUID.new.generate(:compact))
      end
      
      def exceeded_broker_timeouts?
        @broker_timeouts > @max_broker_timeouts
      end

      def reopen_socket
        @reactor.close_socket(@socket) if @socket
        allocate_socket
        on_attach(@socket)
      end
    end # class Handler


    # Spawns its own reactor and acts completely autonomously in its own thread.
    #
    class Client
      attr_reader :reactor

      def initialize(configuration)
        @reactor = ZM::Reactor.new(configuration)
        @reactor.run
        configuration.reactor(@reactor)
        @handler = RzmqBrokers::Client::Handler.new(configuration)
      end

      def send_request(service_name, payload)
        # this method could be called by any thread, so we need to serialize the call back
        # onto the reactor thread so that we aren't doing a socket operation from a
        # non-reactor thread. 0mq gets very upset if we try.
        #
        schedule { @handler.send_request(service_name, payload) }
      end

      def process_request(message, request_options = nil)
        # this method could be called by any thread, so we need to serialize the call back
        # onto the reactor thread so that we aren't doing a socket operation from a
        # non-reactor thread. 0mq gets very upset if we try.
        #
        schedule { @handler.process_request(message, request_options) }
      end


      private

      def schedule(&blk)
        @reactor.next_tick(blk)
      end
    end

  end # module Client
end # module RzmqBrokers
