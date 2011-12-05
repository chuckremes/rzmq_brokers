module RzmqBrokers
  module Client

    # Manages the lifecycle of a Client request.
    #
    # When a reply is received, the Request begins its shutdown. Any active
    # timer is canceled and the reply is passed back to the handler for
    # delivery to the appropriate #on_success or #on_failure method.
    #
    # For a timeout, the #on_failure method is invoked with an empty 
    # ClientReplyFailure message. The client can deduce from the empty
    # payload that the request exceeded its timeout and retries.
    #
    class ActiveRequest
      attr_reader :service_name, :sequence_id, :created_at, :failed_at, :payload, :options

      def initialize(reactor, handler, service_name, sequence_id, payload, options)
        @reactor = reactor
        @handler = handler
        @service_name = service_name
        @sequence_id = sequence_id
        @payload = payload
        @options = options
        @timeout_ms = options.timeout_ms
        @retries = options.retries
        
        @try_count = 0
        @timer = nil
        @created_at = Time.now
        
        resend
      end
      
      def resend
        @try_count += 1
        @reactor.log(:info, "#{self.class}, Request id [#{@sequence_id.inspect}] sent [#{@try_count}] times.")
        @handler.send_request(@service_name, @sequence_id, @payload)
        set_timer if timeout_desired?
      end
      
      def timeout_desired?() @timeout_ms && @timeout_ms > 0; end
      
      def set_timer
        @timer = @reactor.oneshot_timer(@timeout_ms) { on_timeout }
      end

      def process_reply(reply)
        cancel_timer

        if reply.success_reply?
          @reactor.log(:debug, "#{self.class}, Processing successful reply.")
          @handler.on_success(self, reply)
        elsif reply.failure_reply?
          @reactor.log(:debug, "#{self.class}, Processing failed reply.")
          @handler.on_failure(self, reply)
        else
          @reactor.log(:error, "#{self.class}, Processing UNKNOWN reply.")
          @reactor.log(:error, reply.inspect)
        end
      end

      def retries_exceeded?
        @try_count >= @retries
      end

      def on_timeout
        @failed_at = Time.now
        @timer = nil
        @reactor.log(:warn, "#{self.class}, Request id [#{@sequence_id.inspect}] timed out at [#{@failed_at}].")

        if retries_exceeded?
          @reactor.log(:warn, "#{self.class}, Request id [#{@sequence_id.inspect}] retries exceeded; force fail.")
          @handler.on_failure(self)
        else
          resend
        end
      end
      
      def force_close
        cancel_timer
      end
      
      
      private
      
      def cancel_timer
        @timer.cancel if @timer
      end
    end # Request

  end
end
