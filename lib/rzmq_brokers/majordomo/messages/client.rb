
module RzmqBrokers
  module Majordomo
    module Messages


      # 0  - protocol header
      # 1  - REQUEST
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

    end
  end
end
