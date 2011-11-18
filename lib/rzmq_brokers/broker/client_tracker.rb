module RzmqBrokers
  module Broker

    # Takes a first look at each client message and records certain
    # details about it. If the details meet protocol requirements,
    # the message is accepted else it's rejected.
    #
    # Enforces the requirement that all client messages must increment
    # the sequence_number component of the sequence_id monotonically.
    # Any gap indicates that the Broker missed a message, so it starts
    # rejecting the messages. The client should detect this and reopen
    # a new socket which will in turn create a new client_id and reset
    # sequence_number to 1.
    #
    # It also guards against simple hijack attempts. If a client on another
    # socket generates the same client_id as an existing client and matches
    # its sequence_number, it still cannot fake the socket_id assigned by
    # 0mq. A mismatch there will reject the message.
    #
    # Lastly, this class provides a utility for purging old and expired
    # clients that have not sent a request to the broker in a "long"
    # time. This purge keeps lookups fast and memory usage reasonable.
    #
    class ClientTracker
      def initialize(reactor, expire_after_seconds)
        @reactor = reactor
        @expire_after_seconds = expire_after_seconds
        
        @clients = {}
        @timer = @reactor.periodical_timer(3 * 1_000) { purge_expired_clients }
      end

      # Takes a look at the message's origin and sequence_id to verify
      # it conforms to the client/broker protocol.
      #
      def valid_source?(message)
        sequence_number, client_id = message.sequence_id
        client_socket_id = message.envelope_identity

        if (client = @clients[client_id])
          if client.expected?(client_socket_id, sequence_number)
            client.advance_to_next_expectation
            true
          else
            false
          end
        else
          if 1 == sequence_number
            @clients[client_id] = ClientIdentity.new(client_socket_id, client_id)
          else
            false
          end
        end
      end
      
      # Removes any clients that have not been updated in the last
      # +expire_after_seconds+ seconds that was set in #initialize.
      #
      def purge_expired_clients
        oldest_allowed_time = Time.now - @expire_after_seconds
        
        @clients.keys.each do |key|
          @clients.delete(key) if @clients[key].expired?(oldest_allowed_time)
        end
      end
    end # class ClientTracker


    # Stores the unique details of each client connection.
    #
    class ClientIdentity
      def initialize(socket_id, client_id)
        @socket_id = socket_id
        @expected_sequence_number = 2
        @client_id = client_id
        @updated_at = Time.now
      end

      def expected?(socket_id, sequence_number)
        @expected_sequence_number == sequence_number &&
        @socket_id == socket_id
      end

      def advance_to_next_expectation
        @updated_at = Time.now
        @expected_sequence_number += 1
      end
      
      def expired?(oldest_allowed_time)
        @updated_at < oldest_allowed_time
      end
    end # class ClientIdentity

  end # module Broker
end # module RzmqBrokers
