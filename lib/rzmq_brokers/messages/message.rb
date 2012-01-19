
module RzmqBrokers
  module Messages

    module MessageClassMethods

      def envelope_strings(envelope)
        envelope.map { |address| address.copy_out_string }
      end

      # Takes 2 packed 4-byte big-endian longs and a single 4-byte big-endian 
      # long and converts it back into a 64-bit integer and a 32-bit client ID.
      #
      # Returns an array: [64-bit sequence number, 32-bit client id]
      #
      def sequence_decoder(string)
        array = string.unpack('N3')
        [(array[0] << 32) + array[1], array[2]]
      end

      def sequence_encoder(array)
        [array[0] >> 32, array[0], array[1]].pack('N3')
      end

      def decode_64bit(string)
        array = string.unpack('NN')
        (array[0] << 32) + array[1]
      end

      def encode_64bit(number)
        [number >> 32, number].pack('NN')
      end

      def heartbeat_interval_decoder(string)
        string.unpack('N').at(0)
      end

      def heartbeat_interval_encoder(number)
        [number].pack('N')
      end

      def heartbeat_retries_decoder(string)
        string.unpack('C').at(0)
      end

      def heartbeat_retries_encoder(number)
        [number].pack('C')
      end

      # Helper method to create a hex string of a byte sequence.
      #
      def strhex(str)
        hex_chars = "0123456789ABCDEF"
        msg_size = str.size

        result = ""
        str.each_byte do |num|
          i1 = num >> 4
          i2 = num & 15
          result << hex_chars[i1]
          result << hex_chars[i2]
        end
        result
      end

      def delimiter
        [enclose_msg(nil)]
      end
      
      def enclose_msg(string)
        ZMQ::Message.new(string)
      end
    end # module MessageClassMethods


    module MessageInstanceMethods
      attr_reader :service_name, :payload, :address

      # Used for if/else tests. Subclasses of message override these methods and return
      # true where appropriate.
      #
      def client?() false; end
      def worker?() false; end

      def heartbeat?() false; end
      def request?() false; end
      def failure_reply?() false; end
      def success_reply?() false; end
      def disconnect?() false; end

      # Use second-to-last message as the identity of the message source.
      # This value is set by 0mq and should be unique.
      #
      def envelope_identity
        @envelop_identity ||= self.class.strhex(@address[-2]) if @address
      end

      def to_msgs
        [protocol_version_msg]
      end

      def envelope_msgs
        if @address
          @address.map { |address| ZMQ::Message.new(address) }
        else
          []
        end
      end

      def service_name_msg
        ZMQ::Message.new(@service_name)
      end

      def sequence_id_msg
        ZMQ::Message.new(self.class.sequence_encoder(@sequence_id))
      end

      def payload_msgs
        if @payload.is_a?(Array)
          @payload.map { |part| ZMQ::Message.new(part) }
        else
          # payload is not a message part; treat as nil
          [ZMQ::Message.new]
        end
      end
      
      # Empty placeholder so that Client::Handler.process_request works with a regular
      # RzmqBrokers::Messages::Request as input. The RzmqBrokers::Client::Requests
      # class needs each message it receives to answer to #service_name, #sequence_id
      # and #encode. This placeholder satisfies the final requirement.
      def encode
        # no op
      end
    end # module MessageInstanceMethods


  end
end
