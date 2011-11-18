
module RzmqBrokers
  module Consensus
    module Messages


      # methods ending in #_msg return a ZMQ::Message instance
      # methods ending in #_msgs return an array which may contain ZMQ::Message instances
      class ClientMessage < Message

        def self.from_network(frames, envelope)
          service_name = frames.at(2).copy_out_string
          sequence_id = sequence_decoder(frames.at(3).copy_out_string)
          payload = extract_payload(frames)
          new(service_name, sequence_id, payload, envelope)
        end

        def self.extract_payload(frames)
          if frames.size > 4
            frames[4..-1].map { |msg| msg.copy_out_string }
          end
        end

        attr_reader :sequence_id

        def initialize(service_name, sequence_id, payload, envelope = nil)
          @service_name = service_name
          @sequence_id = sequence_id
          @payload = payload
          @envelope = envelope
        end

        def client?() true; end

        def protocol_version_msg
          ZMQ::Message.new(CLIENT_PROTOCOL_VERSION)
        end
      end # class ClientMessage


      # 0  - protocol header
      # 1  - CLIENT_REQUEST
      #
      class ClientRequest < ClientMessage

        def request?() true; end

        def client_request_msg
          ZMQ::Message.new(CLIENT_REQUEST)
        end

        def to_msgs
          super + [client_request_msg, service_name_msg, sequence_id_msg] + payload_msgs
        end
      end # class ClientRequest


      # 0  - protocol header
      # 1  - CLIENT_REPLY_FAILURE
      # 2  - service name
      # 3  - sequence number, uint64, big-endian
      # 4+ - reply payload
      #
      class ClientReplyFailure < ClientMessage
        def self.from_request(message)
          new(message.service_name, message.sequence_id, message.payload)
        end

        def failure_reply?() true; end

        def client_reply_failure_msg
          ZMQ::Message.new(CLIENT_REPLY_FAILURE)
        end

        def to_msgs
          super + [client_reply_failure_msg, service_name_msg, sequence_id_msg] + payload_msgs
        end
      end # class ClientReplyFailure

      # 0  - protocol header
      # 1  - CLIENT_REPLY_SUCCESS
      # 2  - service name
      # 3  - sequence number, uint64, big-endian
      # 4+ - reply payload
      #
      class ClientReplySuccess < ClientMessage
        def self.from_network(frames, envelope)
          service_name = frames.at(2).copy_out_string
          sequence_id = sequence_decoder(frames.at(3).copy_out_string)
          payload = extract_payload(frames)
          new(service_name, sequence_id, payload, envelope)
        end

        def self.extract_payload(frames)
          if frames.size > 4
            frames[4..-1].map { |msg| msg.copy_out_string }
          end
        end

        def initialize(service_name, sequence_id, payload, envelope = nil)
          @service_name = service_name
          @sequence_id = sequence_id
          @payload = payload
          @envelope = envelope
        end

        def success_reply?() true; end

        def client_reply_success_msg
          ZMQ::Message.new(CLIENT_REPLY_SUCCESS)
        end

        def to_msgs
          super + [client_reply_success_msg, service_name_msg, sequence_id_msg] + payload_msgs
        end
      end # class ClientReplySuccess

    end
  end
end
