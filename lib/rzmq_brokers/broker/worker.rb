
module RzmqBrokers
  module Broker

    # Used by the Broker to track the state of a connected Worker and maintain heartbeats
    # between itself and the actual Worker.
    #
    class Worker
      attr_reader :service_name, :identity, :envelope
      def initialize(reactor, handler, service_name, identity, heartbeat_interval, heartbeat_retries, envelope)
        @reactor = reactor
        @handler = handler
        @service_name = service_name
        @identity = identity
        @envelope = envelope.dup

        @heartbeat_interval = heartbeat_interval
        @heartbeat_retries = heartbeat_retries
        @heartbeat_timer = nil
        start_heartbeat
        @hb_received_at = Time.now
      end

      def return_address
        @envelope.map { |address| ZMQ::Message.new(address) }
      end

      def start_heartbeat
        @reactor.log(:debug, "#{self.class}, Worker [#{@identity}] for service [#{@service_name}] starting heartbeat with interval [#{@heartbeat_interval}]")
        @heartbeat_timer = @reactor.periodical_timer(@heartbeat_interval) { beat }
      end

      def beat
        @handler.send_worker_heartbeat(self)
      end

      # Called by handler whenever it receives a worker heartbeat. 
      #
      def process_heartbeat(message = nil)
        @reactor.log(:debug, "#{self.class}, On broker, worker [#{@identity}] received a HB message.")
        @hb_received_at = Time.now
      end

      # Called when the worker has sent a DISCONNECT or its heartbeats have timed out.
      #
      def die
        @reactor.log(:info, "#{self.class}, Worker [#{@identity}] is exiting.")
        @heartbeat_timer.cancel
      end

      # True when this worker reference hasn't received a heartbeat from its worker
      # in interval*retries time.
      #
      def expired?
        # convert time to milliseconds so this comparison works correctly
        elapsed = ((Time.now - @hb_received_at) * 1_000)

        if elapsed > (@heartbeat_interval * @heartbeat_retries)
          @reactor.log(:warn, "#{self.class}, Broker Worker [#{@identity}] expiring, last received hb at #{@hb_received_at}")
          true
        else
          false
        end
      end
    end

  end
end
