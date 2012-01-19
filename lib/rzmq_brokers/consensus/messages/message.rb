
module RzmqBrokers
  module Consensus
    module Messages

      PROTOCOL_VERSION = "CCp100"

      # client messages
      REQUEST = [0x03].pack('C')

      # worker messages
      READY = [0x01].pack('C')
      HEARTBEAT = [0x02].pack('C')
      REPLY_SUCCESS = [0x04].pack('C')
      REPLY_FAILURE = [0x05].pack('C')
      DISCONNECT = [0x06].pack('C')
      
      # other messages
      PING = [0x07].pack('C')

      class Message
        include RzmqBrokers::Messages::MessageInstanceMethods
        extend RzmqBrokers::Messages::MessageClassMethods

        def self.create_from(frames, envelope)
          protocol = frames[0].copy_out_string
          msg_type = frames[1].copy_out_string

          address = envelope_strings(envelope)

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
            elsif ping?(msg_type)
              Ping.from_network(frames, address)
            else
              unknown_message_handler(msg_type, frames, address)
            end
          else
            STDERR.print("fatal: UNKNOWN PROTOCOL [#{protocol.inspect}], expected [#{PROTOCOL_VERSION.inspect}] returning nil\n")
          end # correct_protocol?
        end

        def self.correct_protocol?(string) PROTOCOL_VERSION == string; end
        def self.request?(msg_type) REQUEST == msg_type; end

        def self.ready?(msg_type) READY == msg_type; end
        def self.heartbeat?(msg_type) HEARTBEAT == msg_type; end
        def self.reply_success?(msg_type) REPLY_SUCCESS == msg_type; end
        def self.reply_failure?(msg_type) REPLY_FAILURE == msg_type; end
        def self.disconnect?(msg_type) DISCONNECT == msg_type; end
        def self.ping?(msg_type) PING == msg_type; end

        def self.protocol_version_msg() ZMQ::Message.new(PROTOCOL_VERSION); end


        def self.from_network(frames, address)
          service_name = frames.at(2).copy_out_string
          sequence_id = sequence_decoder(frames.at(3).copy_out_string)
          payload = extract_payload(frames)
          new(service_name, sequence_id, payload, address)
        end

        def self.extract_payload(frames)
          if frames.size > 4
            frames[4..-1].map { |msg| msg.copy_out_string }
          end
        end

        attr_accessor :sequence_id

        def initialize(service_name, sequence_id, payload, address = nil)
          @service_name = service_name
          @sequence_id = sequence_id
          @payload = payload
          @address = address
        end

        def client?() false; end
        def worker?() false; end

        def ready?() false; end
        def heartbeat?() false; end
        def disconnect?() false; end
        def request?() false; end
        def success_reply?() false; end
        def failure_reply?() false; end
        def ping?() false; end

        def protocol_version_msg
          ZMQ::Message.new(PROTOCOL_VERSION)
        end
      end # class Message


      class BrokerMessage < Message

        def self.create_from(frames, envelope)
          protocol = frames[0].copy_out_string
          msg_type = frames[1].copy_out_string

          address = envelope_strings(envelope)
          close_messages(envelope)

          if correct_protocol?(protocol)
            if request?(msg_type)
              BrokerRequest.from_network(frames, address)
            elsif reply_success?(msg_type)
              BrokerReplySuccess.from_network(frames, address)
            elsif reply_failure?(msg_type)
              BrokerReplyFailure.from_network(frames, address)
            elsif ping?(msg_type)
              BrokerPing.from_network(frames, address)
            else
              if heartbeat?(msg_type)
                message = Heartbeat.from_network(frames, address)
              elsif ready?(msg_type)
                message = Ready.from_network(frames, address)
              elsif disconnect?(msg_type)
                message = Disconnect.from_network(frames, address)
              else
                message = unknown_message_handler(msg_type, frames, address)
              end
              
              close_messages(frames)
              message
            end
          else
            STDERR.print("fatal: UNKNOWN PROTOCOL [#{protocol.inspect}], expected [#{PROTOCOL_VERSION.inspect}] returning nil\n")
          end # correct_protocol?
        end

        def self.close_messages(frames) frames.each { |frame| frame.close }; end

        def self.from_network(frames, address)
          service_name = frames.at(2).copy_out_string
          sequence_id = sequence_decoder(frames.at(3).copy_out_string)
          new(frames, address, service_name, sequence_id)
        end

        attr_reader :frames

        def initialize(frames, address, service_name, sequence_id)
          @frames = frames
          @address = address
          @service_name = service_name
          @sequence_id = sequence_id
        end
        
        # Releases the ZMQ::Message objects and free native memory.
        #
        def close(frames = nil)
          frames ||= @frames
          frames && frames.each { |frame| frame.close }
        end
      end # BrokerMessage
      
      
      
      # 0  - protocol header
      # 1  - PING
      #
      class Ping < Message

        def self.from_network(frames, address)
          new(address)
        end
        
        def self.to_msgs
          [ZMQ::Message.new(PROTOCOL_VERSION), ZMQ::Message.new(PING)]
        end

        def initialize(address)
          @address = address
        end

        def ping?() true; end

        def ping_msg
          ZMQ::Message.new(PING)
        end

        def to_msgs
          super + [ping_msg]
        end
      end # class Ping

      # A thinner version of the Ping class used exclusively on the
      # broker. It saves the undecoded frames for forwarding this message on
      # to its eventual destination so we can avoid the cost of re-encoding.
      #
      class BrokerPing < BrokerMessage
        
        def self.from_network(frames, address)
          new(frames, address)
        end
        
        def initialize(frames, address)
          @frames = frames
          @address = address
          @frames = envelope_msgs + @frames
        end

        def ping?() true; end

        def to_msgs
          @frames
        end
      end # class BrokerPing
      

    end
  end
end
