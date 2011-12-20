
module RzmqBrokers
  module Majordomo
    module Messages


      # 0  - protocol header
      # 1  - REQUEST
      # 2  - service name
      # 3  - sequence ID, 16-byte uuid + uint64, big-endian
      # 4+ - request payload
      #
      # methods ending in #_msg return a ZMQ::Message instance
      # methods ending in #_msgs return an array which may contain ZMQ::Message instances
      #
      class Request < Message

        def client?() true; end

        def request?() true; end

        def request_msg
          ZMQ::Message.new(REQUEST)
        end

        def to_msgs
          super + [request_msg, service_name_msg, sequence_id_msg] + payload_msgs
        end
      end # class Request


      # A thinner version of Request used on the Broker only. It saves the original
      # message frames to avoid the costs of copying and re-encoding the data.
      #
      class BrokerRequest < BrokerMessage

        def client?() true; end

        def request?() true; end

        # Can only be called a single time since the returned ZMQ::Message objects
        # will be closed by a call to #write somewhere along the line.
        #
        def to_msgs
          @frames
        end
      end # BrokerRequest

    end
  end
end
