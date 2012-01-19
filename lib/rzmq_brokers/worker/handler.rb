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
        @klass_name = self.class.to_s

        @broker_timer = @heartbeat_timer = nil
        @ready = false

        raise "Worker [#{@service_name}] is misconfigured!" unless @service_name && @on_request && @on_disconnect && @heartbeat_interval
        send_readiness_to_broker
      end

      def on_read(socket, messages, envelope)
        message = @base_msg_klass.create_from(messages, envelope)
        #@reactor.log(:debug, "#{@klass_name}, Reading message #{message.inspect}")

        if message
          @hb_received_at = Time.now # receiving *any* message resets the heartbeat timer

          if message.request?
            process_request(message)
          elsif message.heartbeat?
            process_heartbeat(message)
          elsif message.disconnect?
            @on_disconnect.call(message)
          else
            STDERR.print("#{@klass_name} Unknown message type! #{message.inspect}\n")
            @reactor.log(:error, "#{@klass_name} Unknown message type! #{message.inspect}")
          end
        else
          print_fatal(messages, envelope)
        end
      end

      def send_readiness_to_broker
        @ready = true
        message = @ready_msg_klass.new(@service_name, @heartbeat_interval, @heartbeat_retries)
        @reactor.log(:info, "#{@klass_name}, Sending READY for service [#{@service_name}] with HB interval [#{@heartbeat_interval}] and retries [#{@heartbeat_retries}]")
        write_messages(@base_msg_klass.delimiter + message.to_msgs)
        start_heartbeat
        start_broker_timer
      end

      def disconnect_from_broker
        message = @disconnect_msg_klass.new(@service_name)
        @reactor.log(:info, "#{@klass_name}, Sending DISCONNECT for [#{@service_name}]; canceling broker timer.")
        write_messages(@base_msg_klass.delimiter + message.to_msgs)
        cancel_broker_timer
        @ready = false
      end
      
      def shutdown
        @reactor.log(:info, "#{@klass_name}, Shutting down...")
        if @ready
          disconnect_from_broker
          cancel_broker_timer
          cancel_heartbeat_timer
        end
        
        super()
      end

      # Track the time of the last received heartbeat from the Broker. Used to
      # determine Broker health.
      #
      def process_heartbeat(message)
        @reactor.log(:debug, "#{@klass_name}, Worker [#{@service_name}] received HB from broker at [#{@hb_received_at}].")
      end

      def process_request(message)
        @reactor.log(:debug, "#{@klass_name}, Worker [#{@service_name}] received work request.")
        @on_request.call(self, message)
      end

      def send_success_reply_to_broker(sequence_id, payload)
        @reactor.log(:debug, "#{@klass_name}, Worker [#{@service_name}] sending a successful reply to broker.")
        reply = @base_msg_klass.delimiter + @reply_success_msg_klass.new(@service_name, sequence_id, payload).to_msgs
        write_messages(reply)
      end

      def send_failure_reply_to_broker(sequence_id, payload)
        @reactor.log(:debug, "#{@klass_name}, Worker [#{@service_name}] sending a failure reply to broker.")
        reply = @base_msg_klass.delimiter + @reply_failure_msg_klass.new(@service_name, sequence_id, payload).to_msgs
        write_messages(reply)
      end

      # Called by the broker timer. Verifies that a heartbeat was received within the last
      # time interval. If yes, all is well. If no, we close the connection to the broker
      # and open a new one.
      def broker_check
        unless ((Time.now - @hb_received_at) * 1_000) <= (@heartbeat_interval * @heartbeat_retries)
          @reactor.log(:warn, "#{@klass_name}, Worker [#{@service_name}] has expiring broker, last saw a heartbeat at [#{@hb_received_at}] and is now [#{Time.now}]")
          reconnect_broker
        else
          @reactor.log(:info, "#{@klass_name}, Worker [#{@service_name}] has healthy broker, last saw a heartbeat at [#{@hb_received_at}] and is now [#{Time.now}]")
        end
      end

      def succeeded(sequence_id, payload)
        @reactor.log(:debug, "#{@klass_name}, Sending a success reply.")
        send_success_reply_to_broker(sequence_id, payload)
      end

      def failed(sequence_id, payload)
        @reactor.log(:debug, "Worker, sending a failure reply.")
        send_failure_reply_to_broker(sequence_id, payload)
      end


      private

      def write_messages(messages)
        @hb_sent_at = Time.now
        @reactor.log(:debug, "#{@klass_name}, Sending a message and updating its hb_sent_at to [#{@hb_sent_at}].")
        write(messages)
      end

      # Sends one immediately and schedules a recurring timer to send future
      # heartbeats.
      #
      def start_heartbeat
        @hb_received_at = Time.now
        send_heartbeat
        @heartbeat_timer = @reactor.periodical_timer(@heartbeat_interval) { send_heartbeat }
      end

      def send_heartbeat
        # do not send more than 1 HB per interval; this check is necessary because *other*
        # messages sent to the broker (READY, REPLY) are equivalent to heartbeats
        if ((Time.now - @hb_sent_at) * 1_000) >= @heartbeat_interval
          message = @heartbeat_msg_klass.new
          @reactor.log(:debug, "#{@klass_name}, Sending HB for service [#{@service_name}]")
          write_messages(@base_msg_klass.delimiter + message.to_msgs)
        end
      end

      def start_broker_timer
        interval = @heartbeat_interval * @heartbeat_retries
        @reactor.log(:debug, "#{@klass_name}, Worker [#{@service_name}] will check broker health every [#{interval}] milliseconds.")
        @broker_timer = @reactor.periodical_timer(interval) { broker_check }
      end

      def reconnect_broker
        @reactor.log(:warn, "#{@klass_name}, Worker [#{@service_name}] exceeded broker tries; reconnecting!")

        cancel_heartbeat_timer
        cancel_broker_timer
        
        reopen_socket
        send_readiness_to_broker
      end

      def reopen_socket
        @reactor.close_socket(@socket) if @socket
        allocate_socket
        on_attach(@socket)
      end
      
      def cancel_heartbeat_timer
        if @heartbeat_timer
          canceled = @heartbeat_timer.cancel

          unless canceled
            @reactor.log(:error, "#{@klass_name} Worker could not cancel heartbeat timer! #{@heartbeat_timer.inspect}. Confirm that this object is only accessed by the reactor thread that created it. [#{@reactor.name}] vs [#{Thread.current['reactor-name']}]")
            @reactor.list_timers
          end

          @heartbeat_timer = nil
        end
      end
      
      def cancel_broker_timer
        if @broker_timer
          canceled = @broker_timer.cancel
          unless canceled
            @reactor.log(:error, "#{@klass_name} Worker could not cancel broker timer! #{@broker_timer.inspect}. Confirm that this object is only accessed by the reactor thread that created it. [#{@reactor.name}] vs [#{Thread.current['reactor-name']}]")
            @reactor.list_timers
          end
          
          @broker_timer = nil
        end
      end

      def configure_messages_classes(config)
        @base_msg_klass = config.base_msg_klass.const_get('Message')
        parent = config.base_msg_klass
        @heartbeat_msg_klass = parent.const_get("Heartbeat")
        @reply_success_msg_klass = parent.const_get("ReplySuccess")
        @reply_failure_msg_klass = parent.const_get("ReplyFailure")
        @ready_msg_klass = parent.const_get("Ready")
        @disconnect_msg_klass = parent.const_get("Disconnect")
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
    class Worker
      attr_reader :reactor

      def initialize(configuration)
        @reactor = ZM::Reactor.new(configuration)
        @reactor.run
        configuration.reactor(@reactor)

        schedule { finish_configuration(configuration) }
      end

      def disconnect_from_broker
        schedule { @handler.disconnect_from_broker }
      end
      
      def shutdown
        schedule { @handler.shutdown }
      end

      def succeeded(sequence_id, payload)
        schedule { @handler.succeeded(sequence_id, payload) }
      end

      def failed(sequence_id, payload)
        schedule { @handler.failed(sequence_id, payload) }
      end


      private

      def finish_configuration(configuration)
        @handler = RzmqBrokers::Worker::Handler.new(configuration)
      end

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
