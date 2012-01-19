module RzmqBrokers
  module Broker

    # For use within an existing Reactor
    #
    class Handler

      class Router
        include ZMQMachine::Server::XREP

        def initialize(configuration)
          @config = configuration
          @broker = @config.broker

          server_config = ZMQMachine::Server::Configuration.create_from(@config)
          server_config.on_read = method(:on_read)
          server_config.bind = @config.broker_bind
          server_config.connect = @config.broker_connect
          server_config.endpoint = @config.broker_endpoint
          @base_msg_klass = @config.base_msg_klass.const_get('BrokerMessage')
          super(server_config)
        end

        def on_read(socket, messages, envelope)
          message = @base_msg_klass.create_from(messages, envelope)

          if message
            if message.client?
              @broker.process_client(message)
            elsif message.worker?
              @broker.process_worker(message)
            elsif message.ping?
              @broker.process_ping(message)
            else
              @reactor.log(:error, "#{self.class}, Received unknown message type! #{message.inspect}")
            end
          else
            print_fatal(messages, envelope)
          end
        end


        private

        # Override the method from the ZMQMachine::Server module. We want to keep
        # the message objects around for zero-copy retransmission to avoid the
        # cost of building a new class, allocating native memory, and copying
        # the bytes from Ruby-land to that native memory.
        #
        # Note that this prevents both the body *and* envelope from being freed.
        # This responsibility is now on the user code.
        #
        def close_messages(messages)
          #messages.each { |message| message.close }
        end

        def print_fatal(messages, envelope)
          str  = messages.map { |msg| msg.copy_out_string.inspect }
          #str.unshift(@base_msg_klass.strhex(envelope[-2].copy_out_string))
          str.unshift(@reactor.name)
          str.unshift(Thread.current['reactor-name'].to_s)
          str.unshift(@base_msg_klass.to_s)
          str.unshift(caller(0))
          str.flatten!
          @reactor.log(:fatal, str.join("\n"))
        end
      end # class Router



      # Handler.new
      #
      def initialize(configuration)
        @config = configuration
        @reactor = @config.reactor
        @config.broker = self

        @router = Router.new(@config)

        @services = Services.new(@reactor,
        self,
        @config.service_klass || Service,
        @config.worker_klass || Worker)

        @clients = ClientTracker.new(@reactor, @config.client_expiration_secs)

        # we only try to purge expired workers once per second. This is superior to
        # trying to purge them on *every* incoming message. In that situation when
        # there is no activity, expired workers aren't reaped until a new message
        # arrives. One check per second eliminates a lot of work at the expense of
        # allowing an expired worker to live up to 1s longer than it should.
        @worker_purge_timer = @reactor.periodical_timer(1_000) { purge_expired_workers }

        configure_messages_classes(@config)
        @klass_name = self.class.to_s
      end

      def process_client(message)
        if valid_client?(message)
          if message.request?
            if available_workers?(message.service_name)
              dispatch_client_work(message)
            else
              @reactor.log(:info, "#{@klass_name}, No workers to handle request; failed!")
              send_client_failure(message.envelope_msgs, message)
            end
          else
            @reactor.log(:warn, "#{@klass_name}, Expected a request but received something else! #{message.inspect}")
          end
        else
          @reactor.log(:warn, "#{@klass_name}, Message from invalid client. #{message.inspect}")
        end
      end

      def process_worker(message)
        if message.ready?
          connect_worker(message)
        elsif message.heartbeat?
          process_worker_heartbeat(message)
        elsif message.success_reply? || message.failure_reply?
          process_worker_reply(message)
        elsif message.disconnect?
          worker = @services.find_worker(message.envelope_identity)
          disconnect_worker(worker)
        else
          @reactor.log(:warn, "#{@klass_name}, Received unexpected message type! #{message.inspect}")
        end
      end

      def process_ping(message)
        send_ping(message)
      end

      # Called periodically to purge workers.
      #
      def purge_expired_workers
        @services.purge_expired_workers
      end

      def send_client_failure(return_address, message)
        @reactor.log(:error, "#{@klass_name}, Broker sending a client failure message.")
        @router.write(return_address + @reply_failure_msg_klass.from_request(message).to_msgs)
      end

      def available_workers?(service_name)
        service = @services.find_service_by_name(service_name)

        if service
          # returns true if there are workers, false otherwise
          @reactor.log(:debug, "#{@klass_name}, Found service for [#{service_name}]")
          found = service.workers?
          @reactor.log(:debug, "#{@klass_name}, Workers for [#{service_name}] found? [#{found}]")
          found
        else
          @reactor.log(:warn, "#{@klass_name}, No service found for [#{service_name}]")
          false
        end
      end

      def connect_worker(message)
        worker_identity = message.envelope_identity
        if worker = @services.find_worker(worker_identity)
          # worker was already registered; got READY msg out of sequence
          @reactor.log(:warn, "#{@klass_name}, Worker [#{worker_identity}] already exists; force disconnect.")
          #disconnect_worker(worker)
          #@services.deregister(worker)
        else
          @services.register(message.service_name, worker_identity, message.heartbeat_interval, message.heartbeat_retries, message.address)
          @reactor.log(:info, "#{@klass_name}, Activated worker [#{worker_identity}] for service [#{message.service_name}]")
        end
      end

      def disconnect_worker(worker)
        if worker
          @reactor.log(:info, "#{@klass_name}, Disconnecting a worker [#{worker.identity}] for service [#{worker.service_name}].")
          @router.write(worker.return_address + @disconnect_msg_klass.new(worker.service_name).to_msgs)
          @services.deregister_worker(worker)
        else
          # happens when the broker has been restarted but there are workers running
          # that are shutdown before it times out and reconnects to the broker
          @reactor.log(:info, "#{@klass_name}, Received disconnect message from unknown worker.")
        end
      end

      # A worker can send its requested heartbeat interval and max retries. Each service
      # gets its own minimum heartbeat and maximum retry parameters. Reset the heartbeats
      # for the worker's service if necessary.
      def process_worker_heartbeat(message)
        worker_identity = message.envelope_identity
        if worker = @services.find_worker(worker_identity)
          worker.process_heartbeat(message)
        end
      end

      # pass in the worker; uses worker to build hb message and get return address
      def send_worker_heartbeat(worker)
        @reactor.log(:debug, "#{@klass_name}, Heartbeat for worker [#{worker.identity}] on thread [#{Thread.current['reactor-name']}]")
        @router.write(worker.return_address + @heartbeat_msg_klass.new.to_msgs)
      end

      def send_worker_request(worker, request)
        @reactor.log(:debug, "#{@klass_name}, Sending request to worker [#{worker.identity}]")
        @router.write(worker.format_request(request))
      end

      def send_client_reply_success(return_address, message)
        @reactor.log(:debug, "#{@klass_name}, Sending a successful reply to client.")
        @router.write(return_address + message.to_msgs)
      end

      def send_client_reply_failure(return_address, message, frames)
        @reactor.log(:debug, "#{@klass_name}, Sending a failure reply to client.")

        unless message
          # broker is forcing this failure; build a message from the frames
          message = @reply_failure_msg_klass.from_network(frames, nil)
        end
        @router.write(return_address + message.to_msgs)
      end

      def send_ping(message)
        @reactor.log(:info, "#{@klass_name}, Returning ping.")
        @router.write(message.to_msgs)
      end

      def dispatch_client_work(message)
        @reactor.log(:error, "#{@klass_name}, Called #dispatch_client_work. Should be overridden by subclass!")
      end

      def process_worker_reply(message)
        @reactor.log(:error, "#{@klass_name}, Called #process_worker_reply. Should be overridden by subclass!")
      end


      private

      def configure_messages_classes(config)
        @reactor.log(:error, "#{@klass_name}, Called #configure_messages_classes. Should be overridden by subclass!")
      end

      def valid_client?(message)
        @clients.valid_source?(message)
      end
    end # class Handler


    # Spawns its own reactor and acts completely autonomously in its own thread. Defaults
    # to using the generic RzmqBrokers::Broker::Handler class but that can be
    # overridden by passing in a class name as +handler_klass+.
    #
    class Broker
      attr_reader :reactor

      def initialize(configuration)
        @reactor = ZM::Reactor.new(configuration)
        @reactor.run
        configuration.reactor = @reactor

        # Need to finish setup while on the reactor thread because it will be calling
        # @reactor.log and other reactor-specific methods. These all must be called
        # from the reactor thread or bad things happen.
        @reactor.next_tick { finish_configuration(configuration) }
      end


      private

      def finish_configuration(configuration)
        handler_klass = configuration.broker_klass || RzmqBrokers::Broker::Handler
        @handler = handler_klass.new(configuration)
      end
    end

  end # module Broker
end # module RzmqBrokers
