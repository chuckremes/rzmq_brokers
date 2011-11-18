module RzmqBrokers
  module Worker

    # For use within an existing Reactor
    #
    class Handler
      include ZMQMachine::Server::XREQ

      def initialize(configuration)
        @config = configuration
        @reactor = @config.reactor
        @timeout_ms = @config.timeout_ms
        @heartbeat_interval = @config.heartbeat_interval
        @heartbeat_retries = @config.heartbeat_retries || 3

        @service_name = @config.service_name
        @on_request = @config.on_request
        @on_disconnect = @config.on_disconnect
        configure_messages_classes(@config)

        server_config = ZM::Server::Configuration.create_from(@config)
        server_config.on_read(method(:on_read))
        super(server_config)
        send_readiness_to_broker
      end

      def on_read(socket, messages, envelope)
        message = @base_msg_klass.create_from(messages, envelope)
        @reactor.log(:debug, "Worker, reading message #{message.inspect}")

        @hb_received_at = Time.now # receiving *any* message resets the heartbeat timer

        if message.request?
          process_request(message)
        elsif message.heartbeat?
          process_heartbeat(message)
        elsif message.disconnect?
          @on_disconnect.call(message)
        end
      end

      def send_readiness_to_broker
        message = @worker_ready_msg_klass.new(@service_name)
        @reactor.log(:info, "Worker, sending READY for service [#{@service_name}]")
        write_messages(@base_msg_klass.delimiter + message.to_msgs)
        start_heartbeat
        start_broker_timer
      end

      def disconnect_from_broker
        message = @worker_disconnect_msg_klass.new(@service_name)
        @reactor.log(:info, "Worker, sending DISCONNECT for  [#{@service_name}]; canceling broker timer.")
        write_messages(@base_msg_klass.delimiter + message.to_msgs)
        @broker_timer.cancel
      end

      # Track the time of the last received heartbeat from the Broker. Used to
      # determine Broker health.
      #
      def process_heartbeat(message)
        @reactor.log(:debug, "Worker received HB from broker at [#{@hb_received_at}].")
      end

      def process_request(message)
        @reactor.log(:debug, "Worker received work request.")
        @on_request.call(self, message)
      end

      def send_success_reply_to_broker(sequence_id, payload)
        @reactor.log(:debug, "Worker sending a successful reply to broker.")
        reply = @base_msg_klass.delimiter + @worker_reply_success_msg_klass.new(sequence_id, payload).to_msgs
        write_messages(reply)
      end

      def send_failure_reply_to_broker(sequence_id, payload)
        @reactor.log(:debug, "Worker sending a failure reply to broker.")
        reply = @base_msg_klass.delimiter + @worker_reply_failure_msg_klass.new(sequence_id, payload).to_msgs
        write_messages(reply)
      end

      # Called by the broker timer. Verifies that a heartbeat was received within the last
      # time interval. If yes, all is well. If no, we close the connection to the broker
      # and open a new one.
      def broker_check
        unless ((Time.now - @hb_received_at) * 1_000) <= (@heartbeat_interval * @heartbeat_retries)
          @reactor.log(:warn, "Worker [#{@service_name}] is missing expected heartbeats from broker! Broker timeout!")
          @reactor.log(:warn, "Worker [#{@service_name}] last saw a heartbeat at [#{@hb_received_at}] and is now [#{Time.now}]")
          reconnect_broker
        else
          @reactor.log(:info, "Worker [#{@service_name}] sees a healthy broker, time now [#{Time.now}]")
        end
      end

      def succeeded(sequence_id, payload)
        @reactor.log(:debug, "Worker, sending a success reply.")
        send_success_reply_to_broker(sequence_id, payload)
      end

      def failed(sequence_id, payload)
        @reactor.log(:debug, "Worker, sending a failure reply.")
        send_failure_reply_to_broker(sequence_id, payload)
      end


      private

      def write_messages(messages)
        @hb_sent_at = Time.now
        write(messages)
      end

      # Sends one immediately and schedules a recurring timer to send future
      # heartbeats.
      #
      def start_heartbeat
        @hb_received_at = Time.now
        send_heartbeat
        @heartbeat_timer = @reactor.periodical_timer(@heartbeat_interval) do
          send_heartbeat
        end
      end

      def send_heartbeat
        # do not send more than 1 HB per interval; this check is necessary because *other*
        # messages sent to the broker (READY, REPLY) are equivalent to heartbeats
        if ((Time.now - @hb_sent_at) * 1_000) >= @heartbeat_interval
          message = @worker_heartbeat_msg_klass.new(@heartbeat_interval, @heartbeat_retries)
          @reactor.log(:debug, "Worker, sending HEARTBEAT")
          write_messages(@base_msg_klass.delimiter + message.to_msgs)
        end
      end

      def start_broker_timer
        @broker_timer = @reactor.periodical_timer(@heartbeat_interval * @heartbeat_retries) { broker_check }
      end

      def reconnect_broker
        @reactor.log(:warn, "Worker exceeded broker tries; reconnecting!")
        @heartbeat_timer.cancel if @heartbeat_timer
        @broker_timer.cancel if @broker_timer
        reopen_socket
        send_readiness_to_broker
      end

      def reopen_socket
        @reactor.close_socket(@socket) if @socket
        allocate_socket
        on_attach(@socket)
      end

      def configure_messages_classes(config)
        @base_msg_klass = config.base_msg_klass.const_get('Message')
        parent = config.base_msg_klass
        @worker_heartbeat_msg_klass = parent.const_get("WorkerHeartbeat")
        @worker_reply_success_msg_klass = parent.const_get("WorkerReplySuccess")
        @worker_reply_failure_msg_klass = parent.const_get("WorkerReplyFailure")
        @worker_ready_msg_klass = parent.const_get("WorkerReady")
        @worker_disconnect_msg_klass = parent.const_get("WorkerDisconnect")
      end
    end # class Handler


    # Spawns its own reactor and acts completely autonomously in its own thread.
    #
    class Worker
      attr_reader :reactor

      def initialize(configuration)
        @reactor = ZM::Reactor.new(configuration)
        @reactor.run
        configuration.reactor(@reactor)
        @handler = RzmqBrokers::Worker::Handler.new(configuration)
      end

      def disconnect
        schedule { @handler.disconnect_from_broker }
      end

      def succeeded(sequence_id, payload)
        schedule { @handler.succeeded(sequence_id, payload) }
      end

      def failed(sequence_id, payload)
        schedule { @handler.failed(sequence_id, payload) }
      end


      private
      
      # Everything called on this instance needs to be rescheduled on the
      # reactor thread. Failure to do so will result in threading bugs, race
      # conditions, etc.
      #
      def schedule(&blk)
        @reactor.next_tick(blk)
      end
    end

  end # module Worker
end # module RzmqBrokers
