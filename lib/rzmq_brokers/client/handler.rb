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
        @on_ping = @config.on_ping

        @requests = Requests.new(@reactor, self)

        server_config = ZM::Server::Configuration.create_from(@config)
        server_config.on_read(method(:on_read))
        super(server_config)
      end

      def on_read(socket, messages, envelope)
        message = @base_msg_klass.create_from(messages, envelope)
        #@reactor.log(:debug, "Client reply, success_reply? [#{message.success_reply?}], failure_reply? [#{message.failure_reply?}], msg.inspect #{message.inspect}")

        if message
          if message.success_reply? || message.failure_reply?
            @requests.process_reply(message)
          elsif message.ping?
            @on_ping.call
          end
        else
          print_fatal(messages, envelope)
        end
      end
      
      def shutdown
        @reactor.log(:info, "#{self.class}, Shutting down...")
        super()
      end

      # Shouldn't be called directly by user code. This is a helper called via #process_request
      # and from the Request#resend method.
      #
      def send_request(service_name, sequence_id, payload)
        message = @request_msg_klass.new(service_name, sequence_id, payload)
        @reactor.log(:debug, "#{self.class}, Sending request #{message.inspect}")
        write(@base_msg_klass.delimiter + message.to_msgs)
      end

      def process_request(message, request_options = nil)
        request_options ||= RequestOptions.new
        message.sequence_id = get_sequence_id
        @reactor.log(:debug, "#{self.class}, Processing request #{message.inspect}")
        @requests.add(message, request_options)
      end

      def send_ping
        @reactor.log(:debug, "#{self.class}, Processing ping")
        write(@base_msg_klass.delimiter + @ping_msg_klass.to_msgs)
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
            message = @reply_failure_msg_klass.new(request.service_name, request.sequence_id, nil)
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
        @reactor.log(:warn, "#{self.class}, Client exceeded allowable [#{@max_broker_timeouts}] timeout failures; reopening socket to Broker!")

        # active requests that haven't timed out & failed will still have the old
        # client ID; we need to restart those requests with the new ID
        @broker_timeouts = 0
        reopen_broker_connection
      end

      def reopen_broker_connection
        @reactor.log(:info, "#{self.class}, Requesting a new connection to the Broker.")
        reopen_socket
        @client_id = generate_client_id
        @sequence_id = 1
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
        @request_msg_klass = parent.const_get('Request')
        @reply_success_msg_klass = parent.const_get('ReplySuccess')
        @reply_failure_msg_klass = parent.const_get('ReplyFailure')
        @ping_msg_klass = parent.const_get('Ping')
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

      def print_fatal(messages, envelope)
        str  = messages.map { |msg| msg.copy_out_string.inspect }
        #str.unshift(@base_msg_klass.strhex(envelope[-2].copy_out_string))
        str.unshift(@reactor.name)
        str.unshift(Thread.current['reactor-name'].to_s)
        str.unshift(@base_msg_klass.to_s)
        str.unshift(caller(0))
        str.flatten!
        STDERR.print(str.join("\n"))
        @reactor.log(:fatal, str.join("\n"))
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

        schedule { finish_configuration(configuration) }
      end

      def send_request(service_name, payload)
        # this method could be called by any thread, so we need to serialize the call back
        # onto the reactor thread so that we aren't doing a socket operation from a
        # non-reactor thread. 0mq gets very upset if we try.
        #
        schedule { @handler.send_request(service_name, payload) }
      end

      # Takes the request +message+ and the +request_options+ and sends
      # them to the broker. This method is also *responsible* for assigning
      # a +sequence_id+ to the message. If the message already has one, it is
      # overwritten here.
      #
      def process_request(message, request_options = nil)
        # this method could be called by any thread, so we need to serialize the call back
        # onto the reactor thread so that we aren't doing a socket operation from a
        # non-reactor thread. 0mq gets very upset if we try.
        #
        schedule { @handler.process_request(message, request_options) }
      end

      def send_ping
        schedule { @handler.send_ping }
      end

      def reopen_broker_connection
        schedule { @handler.reopen_broker_connection }
      end
      
      def shutdown
        schedule { @handler.shutdown }
      end


      private

      def finish_configuration(configuration)
        @handler = RzmqBrokers::Client::Handler.new(configuration)
      end

      def schedule(&blk)
        @reactor.next_tick(blk)
      end
    end

  end # module Client
end # module RzmqBrokers
