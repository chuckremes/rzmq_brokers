
module RzmqBrokers
  module Broker

    # Used by the Broker to track the state of a connected Worker and maintain heartbeats
    # between itself and the actual Worker.
    #
    class Worker
      attr_reader :service_name, :identity, :envelope
      def initialize(reactor, handler, service_name, identity, envelope)
        @reactor = reactor
        @handler = handler
        @service_name = service_name
        @identity = identity
        @envelope = envelope

        @heartbeat_interval = 10_000
        @heartbeat_retries = 3
        @heartbeat_timer = nil
        start_heartbeat
        @hb_received_at = Time.now
      end

      def return_address
        @envelope.map { |address| ZMQ::Message.new(address) }
      end

      def start_heartbeat
        @reactor.log(:debug, "Worker [#{@identity}] for service [#{@service_name}] starting heartbeat with interval [#{@heartbeat_interval}]")
        @heartbeat_timer = @reactor.periodical_timer(@heartbeat_interval) { beat }
      end

      def beat
        @handler.send_worker_heartbeat(self, @heartbeat_interval, @heartbeat_retries)
      end

      # Called by handler whenever it receives a worker heartbeat. Resets the heartbeat timer if
      # the interval or retries inside this message are acceptable, otherwise the timer is
      # left alone.
      def process_heartbeat(message = nil)
        if message && (message.heartbeat_interval < @heartbeat_interval || message.heartbeat_retries > @heartbeat_retries)
          @reactor.log(:debug, "Resetting Worker [#{@identity}] HB interval and/or retries")

          @heartbeat_interval = message.heartbeat_interval if message.heartbeat_interval < @heartbeat_interval
          @heartbeat_retries = message.heartbeat_retries if message.heartbeat_retries > @heartbeat_retries

          @heartbeat_timer.cancel if @heartbeat_timer
          start_heartbeat
        end

        @hb_received_at = Time.now
      end

      # Called when the worker has sent a DISCONNECT or its heartbeats have timed out.
      #
      def die
        @reactor.log(:info, "Worker [#{@identity}] is exiting.")
        @heartbeat_timer.cancel
      end

      # True when this worker reference hasn't received a heartbeat from its worker
      # in interval*retries time.
      #
      def expired?
        # convert time to milliseconds so this comparison works correctly
        elapsed = ((Time.now - @hb_received_at) * 1_000)

        if elapsed > (@heartbeat_interval * @heartbeat_retries)
          @reactor.log(:warn, "Broker Worker [#{@identity}] expiring, last received hb at #{@hb_received_at}")
          true
        else
          false
        end
      end
    end

  end
end
