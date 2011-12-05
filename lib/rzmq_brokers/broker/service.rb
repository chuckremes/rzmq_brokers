
module RzmqBrokers
  module Broker

    # Used by the Broker to maintain a list of Workers for the named Service. The Service
    # knows how to iterate, add, delete and purge workers.
    #
    # This is meant to be subclassed to add specific support for a protocol (e.g. Consensus)
    #
    class Service
      include Enumerable
      attr_reader :name

      def initialize(reactor, name, handler)
        @reactor = reactor
        @name = name
        @handler = handler
        @workers = Hash.new # key is identity, value is Worker instance
      end

      # It is safe to call other Service methods on the yeilded workers
      # even when adding or deleting a worker. This method does the
      # right thing.
      #
      def each
        # by calling #values, we are working on a copy; any addition/deletion
        # will happen on @workers
        @workers.values.each { |worker| yield(worker) }
      end

      def add(worker)
        @workers[worker.identity] = worker
        @reactor.log(:debug, "#{self.class}, Service [#{name}] adding worker, [#{worker_count}] total workers.")
      end

      def delete(worker)
        @workers.delete(worker.identity)
        worker.die
        @reactor.log(:debug, "#{self.class}, Service [#{name}] deleting worker, [#{worker_count}] remaining workers.")
      end

      def [](identity)
        @workers[identity]
      end

      def workers?
        @workers.size > 0
      end

      def worker_count
        @workers.size
      end

      def purge_expired_workers
        @workers.values.each do |worker|
          delete(worker) if worker.expired?
        end
      end
      
      # Any message from a worker is considered part of the heartbeat. Make sure
      # the worker is updated so it doesn't expire.
      #
      # Subclasses of this class should be sure to call #super so that this operation
      # is handled.
      #
      def process_reply(message)
        worker = @workers[message.envelope_identity]
        worker.process_heartbeat if worker
        worker
      end

      # Checks message to verify this service can handle it. May be rejected due to
      # incompatibility, duplicate sequence number, sequence number already marked as
      # failed, etc.
      #
      def request_ok?(message)
        true
      end

      def ready?
        true
      end

      def add_request(message)
        # no op
      end
    end # class Service

  end
end
