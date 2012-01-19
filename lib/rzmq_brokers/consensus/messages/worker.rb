
module RzmqBrokers
  module Consensus
    module Messages


      # 0  - protocol header
      # 1  - message type
      # 2  - service name
      # 3  - desired heartbeat interval in milliseconds
      # 4  - desired max retries
      #
      class Ready < Message
        attr_reader :heartbeat_interval, :heartbeat_retries

        def self.from_network(frames, address)
          service_name = frames.at(2).copy_out_string
          heartbeat_interval = heartbeat_interval_decoder(frames.at(3).copy_out_string)
          heartbeat_retries = heartbeat_retries_decoder(frames.at(4).copy_out_string)
          new(service_name, heartbeat_interval, heartbeat_retries, address)
        end

        def initialize(service_name, heartbeat_interval, heartbeat_retries, address = nil)
          @service_name = service_name
          @heartbeat_interval = heartbeat_interval
          @heartbeat_retries = heartbeat_retries
          @address = address
        end

        def worker?() true; end

        def ready?() true; end

        def to_msgs
          super + [ready_msg, service_name_msg, heartbeat_interval_msg, heartbeat_retries_msg]
        end

        def ready_msg
          ZMQ::Message.new(READY)
        end

        def heartbeat_interval_msg
          ZMQ::Message.new(Message.heartbeat_interval_encoder(@heartbeat_interval))
        end

        def heartbeat_retries_msg
          ZMQ::Message.new(Message.heartbeat_retries_encoder(@heartbeat_retries))
        end
      end # class Ready


      # 0  - protocol header
      # 1  - message type
      #
      class Heartbeat < Message

        def self.from_network(frames, address)
          new(address)
        end

        def initialize(address = nil)
          @address = address
        end

        def worker?() true; end

        def heartbeat?() true; end

        def to_msgs
          super + [heartbeat_msg]
        end

        def heartbeat_msg
          ZMQ::Message.new(HEARTBEAT)
        end
      end # class Heartbeat


      # 0  - protocol header
      # 1  - REPLY_SUCCESS
      # 2  - service name
      # 3  - sequence ID, 16-byte uuid + uint64, big-endian
      # 4+ - reply payload
      #
      class ReplySuccess < Message
        def self.from_network(frames, address)
          service_name = frames.at(2).copy_out_string
          sequence_id = sequence_decoder(frames.at(3).copy_out_string)
          payload = extract_payload(frames)
          new(service_name, sequence_id, payload, address)
        end

        def initialize(service_name, sequence_id, payload, address = nil)
          @service_name = service_name
          @sequence_id = sequence_id
          @payload = payload
          @address = address
        end

        def worker?() true; end

        def success_reply?() true; end

        def reply_success_msg
          ZMQ::Message.new(REPLY_SUCCESS)
        end

        def to_msgs
          super + [reply_success_msg, service_name_msg, sequence_id_msg] + payload_msgs
        end
      end # class ReplySuccess

      
      # A thinner version of the ReplySuccess class used exclusively on the
      # broker. It saves the undecoded frames for forwarding this message on
      # to its eventual destination so we can avoid the cost of re-encoding.
      #
      class BrokerReplySuccess < BrokerMessage
        
        def initialize(frames, address, service_name, sequence_id)
          @payload = frames.slice!(4..-1)
          super
        end

        def worker?() true; end

        def success_reply?() true; end

        # Let's the Request potentially replace the already-encoded payload.
        # Replace the 4th frame forward.
        #
        def payload=(replacement)
          close(@payload)
          @payload = replacement
          nil
        end
        
        def payload_frames
          @payload
        end

        def to_msgs
          @frames + @payload
        end
      end # class BrokerReplySuccess


      # 0  - protocol header
      # 1  - REPLY_FAILURE
      # 2  - service name
      # 3  - sequence ID, 16-byte uuid + uint64, big-endian
      # 4+ - reply payload
      #
      class ReplyFailure < Message
        def self.from_request(message)
          new(message.service_name, message.sequence_id, nil)
        end

        def worker?() true; end

        def failure_reply?() true; end

        def reply_failure_msg
          ZMQ::Message.new(REPLY_FAILURE)
        end

        def to_msgs
          super + [reply_failure_msg, service_name_msg, sequence_id_msg] + payload_msgs
        end
      end # class ReplyFailure


      class BrokerReplyFailure < BrokerReplySuccess
        def self.from_request(message)
          frames = message.frames
          service_name = frames.at(2).copy_out_string
          sequence_id = frames.at(3).copy_out_string
          message.close
          frames = [protocol_version_msg, ZMQ::Message.new(REPLY_FAILURE), ZMQ::Message.new(service_name), ZMQ::Message.new(sequence_id)]
          from_network(frames, message.address)
        end

        def failure_reply?() true; end
      end # class BrokerReplyFailure


      # 0  - protocol header
      # 1  - message type
      # 2  - service name
      #
      class Disconnect < Message
        def self.from_network(frames, address)
          service_name = frames.at(2).copy_out_string
          new(service_name, address)
        end

        def initialize(service_name, addresss = nil)
          @service_name = service_name
          @address = address
        end

        def worker?() true; end

        def disconnect?() true; end

        def to_msgs
          super + [disconnect_msg, service_name_msg]
        end

        def disconnect_msg
          ZMQ::Message.new(DISCONNECT)
        end
      end # class Disconnect


    end
  end
end
