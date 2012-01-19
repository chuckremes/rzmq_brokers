
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
        @hb_sent_at = Time.at(0) # set to epoch so first heartbeat goes out
        start_heartbeat
        @hb_received_at = Time.now # true because we just received a READY msg
      end

      def return_address
        @envelope.map { |address| ZMQ::Message.new(address) }
      end
      
      def format_request(request)
        # reset hb_sent_at so that we consider any message (including a request) to be a
        # heartbeat; this prevents us from "overbeating" and wasting bandwidth
        @hb_sent_at = Time.now
        return_address + request.to_msgs
      end

      def start_heartbeat
        @reactor.log(:debug, "#{self.class}, Worker [#{@identity}] for service [#{@service_name}] starting heartbeat with interval [#{@heartbeat_interval}] for reactor [#{@reactor.name}] on thread [#{Thread.current['reactor-name']}]")
        @dead = false
        @heartbeat_timer = @reactor.periodical_timer(@heartbeat_interval) { beat }
      end

      def beat
        now = Time.now
        elapsed = (now - @hb_sent_at) * 1_000 # convert to milliseconds so we are comparing the same "units"
        
        if elapsed >= @heartbeat_interval
          @hb_sent_at = now
          @reactor.log(:debug, "#{self.class}, Worker [#{@identity}] for service [#{@service_name}] is sending a HB to the worker.")
          @handler.send_worker_heartbeat(self)
          @reactor.log(:warn, "#{self.class}, Worker [#{@identity}] is dead for service [#{@service_name}] but the broker is still sending heartbeats! Make sure this object is only ever accessed from its own reactor thread!") if @dead
        end
      end

      # Called by handler whenever it receives a worker heartbeat.
      #
      def process_heartbeat(message = nil)
        @reactor.log(:debug, "#{self.class}, On broker, worker [#{@identity}] for service [#{@service_name}] received a HB message.")
        @hb_received_at = Time.now
      end

      # Called when the worker has sent a DISCONNECT or its heartbeats have timed out.
      #
      def die
        @reactor.log(:info, "#{self.class}, Worker [#{@identity}] for service [#{@service_name}] is exiting.")
        canceled = @heartbeat_timer.cancel

        unless canceled
          @reactor.log(:error, "#{self.class}, Worker [#{@identity}] for service [#{@service_name}] could *not* cancel heartbeat timer on reactor [#{@reactor.name}]. There must be a threading problem somewhere. Make sure this object is *only accessed* from its reactor thread!")
        end

        @dead = true
      end

      # True when this worker reference hasn't received a heartbeat from its worker
      # in interval*retries time.
      #
      def expired?
        # convert time to milliseconds so this comparison works correctly
        elapsed = ((Time.now - @hb_received_at) * 1_000)

        if elapsed > (@heartbeat_interval * @heartbeat_retries)
          @reactor.log(:warn, "#{self.class}, Broker Worker [#{@identity}] for service [#{@service_name}] is expiring, last received hb at #{@hb_received_at}")
          true
        else
          false
        end
      end
    end # Worker

  end
end
