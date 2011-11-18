module RzmqBrokers
  module Client

    # Stores all of the active requests for a Client. When a new request is
    # added, the Request handles the processing. Upon receipt of a reply,
    # this class passes the reply to the appropriate Request and *always*
    # closes the request after the delivery.
    #
    # Replies that are not matched to a Request are dropped.
    #
    class Requests
      def initialize(reactor, handler)
        @reactor = reactor
        @handler = handler
        
        @requests = {}
      end

      # Create a new Request from this +message+ and +options+ and save it.
      #
      def add(message, options)
        readd(message.service_name, message.sequence_id, message.encode, options)
      end

      def process_reply(reply)
        if (request = find_active(reply.sequence_id))
          @reactor.log(:debug, "Client found a request to match reply id #{reply.sequence_id.inspect}")
          request.process_reply(reply)
          close(request)
        else
          @reactor.log(:warn, "Client could not find a request to match reply id #{reply.sequence_id.inspect}")
          #@reactor.log(:debug, @requests.keys.inspect)
        end
      end
      
      # Replaces each active request with a copy of the original request
      # with a new sequence_id.
      #
      def restart_all_with_client_id(client_id)
        @requests.keys.each_with_index do |key, new_sequence_number|
          # create a new request from the current one but with the revised client_id and sequence_number
          request = @requests[key]
          
          new_sequence_number += 1 # because #each_with_index is 0-based
          readd(request.service_name, [new_sequence_number, client_id], request.payload, request.options)
          
          # close the original request and delete it from @requests; no replies can
          # arrive for the original sequence_id since the socket was reopened
          request.force_close
          close(request)
        end
      end
      
      
      private
      
      def readd(service_name, sequence_id, payload, options)
        request = ActiveRequest.new(@reactor, @handler, service_name, sequence_id, payload, options)
        @requests[sequence_id] = request
      end
      
      def find_active(sequence_id)
        @requests[sequence_id]
      end
      
      def close(request)
        @requests.delete(request.sequence_id)
      end
    end # Requests

  end
end
