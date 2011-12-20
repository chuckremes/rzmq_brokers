
module RzmqBrokers
  module Consensus
    module Messages


      # 0  - protocol header
      # 1  - REQUEST
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

        # Called when sending the request to each worker. We want to duplicate this
        # request as cheaply as possible.
        def dup
          frames = @frames.map do |frame|
            dup_frame = ZMQ::Message.new
            rc = dup_frame.copy(frame.pointer)
            dup_frame
          end
#          @dup_frame_strings ||= @frames.map { |frame| frame.copy_out_string }
#          frames = @dup_frame_strings.map { |string| ZMQ::Message.new(string) }
          self.class.new(frames, @address, @service_name, @sequence_id)
        end
      end # BrokerRequest

    end
  end
end
