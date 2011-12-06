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
          @base_msg_klass = @config.base_msg_klass.const_get('Message')
          super(server_config)
        end

        def on_read(socket, messages, envelope)
          message = @base_msg_klass.create_from(messages, envelope)
          #@reactor.log(:debug, "Broker, read message #{message.inspect}")

          if message.client?
            @broker.process_client(message)
          elsif message.worker?
            @broker.process_worker(message)
          end
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
      end

      def process_client(message)
        if valid_client?(message)
          if message.request?
            if available_workers?(message.service_name)
              dispatch_client_work(message)
            else
              @reactor.log(:info, "#{self.class}, No workers to handle request; failed!")
              send_client_failure(message.envelope_msgs, message)
            end
          end
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
        end
      end

      # Called periodically to purge workers.
      #
      def purge_expired_workers
        @services.purge_expired_workers
      end

      def send_client_failure(return_address, message)
        @reactor.log(:error, "#{self.class}, Broker sending a client failure message.")
        @router.write(return_address + @client_reply_failure_msg_klass.from_request(message).to_msgs)
      end

      def available_workers?(service_name)
        service = @services.find_service_by_name(service_name)

        if service
          # returns true if there are workers, false otherwise
          @reactor.log(:debug, "#{self.class}, Found service for [#{service_name}]")
          found = service.workers?
          @reactor.log(:debug, "#{self.class}, Workers for [#{service_name}] found? [#{found}]")
          found
        else
          @reactor.log(:warn, "#{self.class}, No service found for [#{service_name}]")
          false
        end
      end

      def connect_worker(message)
        worker_identity = message.envelope_identity
        if worker = @services.find_worker(worker_identity)
          # worker was already registered; got READY msg out of sequence
          @reactor.log(:warn, "#{self.class}, Worker [#{worker_identity}] already exists; force disconnect.")
          #disconnect_worker(worker)
          #@services.deregister(worker)
        else
          @services.register(message.service_name, worker_identity, message.heartbeat_interval, message.heartbeat_retries, message.envelope.dup)
          @reactor.log(:info, "#{self.class}, Activated worker [#{worker_identity}] for service [#{message.service_name}]")
        end
      end

      def disconnect_worker(worker)
        @reactor.log(:info, "#{self.class}, Disconnecting a worker [#{worker.identity}] for service [#{worker.service_name}].")
        @router.write(worker.return_address + @worker_disconnect_msg_klass.new(worker.service_name).to_msgs)
        @services.deregister_worker(worker)
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
        @reactor.log(:debug, "#{self.class}, Heartbeat for worker [#{worker.identity}]")
        @router.write(worker.return_address + @worker_heartbeat_msg_klass.new.to_msgs)
      end

      def send_worker_request(worker, request)
        @reactor.log(:debug, "#{self.class}, Sending request to worker [#{worker.identity}]")
        @router.write(worker.return_address + @worker_request_msg_klass.from_client_request(request).to_msgs)
      end

      def send_client_reply_success(return_address, service_name, sequence_id, payload)
        @reactor.log(:debug, "#{self.class}, Sending a successful reply to client.")
        @router.write(return_address + @client_reply_success_msg_klass.new(service_name, sequence_id, payload).to_msgs)
      end

      def send_client_reply_failure(return_address, service_name, sequence_id, payload)
        @reactor.log(:debug, "#{self.class}, Sending a failure reply to client.")
        @router.write(return_address + @client_reply_failure_msg_klass.new(service_name, sequence_id, payload).to_msgs)
      end

      def dispatch_client_work(message)
        @reactor.log(:error, "#{self.class}, Called #dispatch_client_work. Should be overridden by subclass!")
      end

      def process_worker_reply(message)
        @reactor.log(:error, "#{self.class}, Called #process_worker_reply. Should be overridden by subclass!")
      end


      private

      def configure_messages_classes(config)
        @reactor.log(:error, "#{self.class}, Called #configure_messages_classes. Should be overridden by subclass!")
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
        handler_klass = configuration.broker_klass || RzmqBrokers::Broker::Handler
        @handler = handler_klass.new(configuration)
      end
    end

  end # module Broker
end # module RzmqBrokers
