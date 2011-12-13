
module RzmqBrokers
  module Majordomo
    module Messages

      PROTOCOL_VERSION = "Mdp200"

      # client messages
      REQUEST = [0x03].pack('C')

      # worker messages
      READY = [0x01].pack('C')
      HEARTBEAT = [0x02].pack('C')
      REPLY_SUCCESS = [0x04].pack('C')
      REPLY_FAILURE = [0x05].pack('C')
      DISCONNECT = [0x06].pack('C')

      class Message
        include RzmqBrokers::Messages::MessageInstanceMethods
        extend RzmqBrokers::Messages::MessageClassMethods

        def self.create_from(frames, envelope)
          protocol = frames[0].copy_out_string
          msg_type = frames[1].copy_out_string

          address = envelope_strings(envelope) if envelope && envelope.respond_to?(:each)

          if correct_protocol?(protocol)
            if request?(msg_type)
              Request.from_network(frames, address)
            elsif reply_success?(msg_type)
              ReplySuccess.from_network(frames, address)
            elsif reply_failure?(msg_type)
              ReplyFailure.from_network(frames, address)
            elsif heartbeat?(msg_type)
              Heartbeat.from_network(frames, address)
            elsif ready?(msg_type)
              Ready.from_network(frames, address)
            elsif disconnect?(msg_type)
              Disconnect.from_network(frames, address)
            else
              unknown_message_handler(msg_type, frames, address)
            end
          else
            #print("UNKNOWN PROTOCOL, return nil\n")
          end # correct_protocol?
        end

        def self.correct_protocol?(string) PROTOCOL_VERSION == string; end
        def self.request?(msg_type) REQUEST == msg_type; end

        def self.ready?(msg_type) READY == msg_type; end
        def self.heartbeat?(msg_type) HEARTBEAT == msg_type; end
        def self.reply_success?(msg_type) REPLY_SUCCESS == msg_type; end
        def self.reply_failure?(msg_type) REPLY_FAILURE == msg_type; end
        def self.disconnect?(msg_type) DISCONNECT == msg_type; end

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

        attr_accessor :sequence_id

        def initialize(service_name, sequence_id, payload, envelope = nil)
          @service_name = service_name
          @sequence_id = sequence_id
          @payload = payload
          @envelope = envelope
        end

        def client?() false; end
        def worker?() false; end

        def ready?() false; end
        def heartbeat?() false; end
        def disconnect?() false; end
        def request?() false; end
        def success_reply?() false; end
        def failure_reply?() false; end

        def protocol_version_msg
          ZMQ::Message.new(PROTOCOL_VERSION)
        end
      end # class Message

    end
  end
end
