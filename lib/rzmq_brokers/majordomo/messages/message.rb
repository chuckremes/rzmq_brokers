
module RzmqBrokers
  module Majordomo
    module Messages

      CLIENT_PROTOCOL_VERSION = "MCp100"
      CLIENT_HEARTBEAT = [0x02].pack('C') # '\002'
      CLIENT_REQUEST = [0x03].pack('C')
      CLIENT_REPLY_SUCCESS = [0x04].pack('C')
      CLIENT_REPLY_FAILURE = [0x05].pack('C')

      WORKER_PROTOCOL_VERSION = "MWp100"
      WORKER_READY = [0x01].pack('C')
      WORKER_HEARTBEAT = [0x02].pack('C')
      WORKER_REQUEST = [0x03].pack('C')
      WORKER_REPLY_SUCCESS = [0x04].pack('C')
      WORKER_REPLY_FAILURE = [0x05].pack('C')
      WORKER_DISCONNECT = [0x06].pack('C')

      class Message
        include RzmqBrokers::Messages::MessageInstanceMethods
        extend RzmqBrokers::Messages::MessageClassMethods

        def self.create_from(frames, envelope)
          protocol = frames[0].copy_out_string
          msg_type = frames[1].copy_out_string

          address = envelope_strings(envelope) if envelope && envelope.respond_to?(:each)

          if client_protocol?(protocol)
            if client_heartbeat?(msg_type)
            elsif client_request?(msg_type)
              ClientRequest.from_network(frames, address)
            elsif client_reply_success?(msg_type)
              ClientReplySuccess.from_network(frames, address)
            elsif client_reply_failure?(msg_type)
              ClientReplyFailure.from_network(frames, address)
            else
              unknown_client_message_handler(msg_type, frames, address)
            end
          elsif worker_protocol?(protocol)
            if worker_ready?(msg_type)
              WorkerReady.from_network(frames, address)
            elsif worker_heartbeat?(msg_type)
              WorkerHeartbeat.from_network(frames, address)
            elsif worker_request?(msg_type)
              WorkerRequest.from_network(frames, address)
            elsif worker_reply_success?(msg_type)
              WorkerReplySuccess.from_network(frames, address)
            elsif worker_reply_failure?(msg_type)
              WorkerReplyFailure.from_network(frames, address)
            elsif worker_disconnect?(msg_type)
              WorkerDisconnect.from_network(frames, address)
            else
              unknown_worker_message_handler(msg_type, frames, address)
            end
          end
        end

        def self.client_protocol?(string) CLIENT_PROTOCOL_VERSION == string; end
        def self.client_heartbeat?(msg_type) CLIENT_HEARTBEAT == msg_type; end
        def self.client_request?(msg_type) CLIENT_REQUEST == msg_type; end
        def self.client_reply_success?(msg_type) CLIENT_REPLY_SUCCESS == msg_type; end
        def self.client_reply_failure?(msg_type) CLIENT_REPLY_FAILURE == msg_type; end

        def self.worker_protocol?(string) WORKER_PROTOCOL_VERSION == string; end
        def self.worker_ready?(msg_type) WORKER_READY == msg_type; end
        def self.worker_heartbeat?(msg_type) WORKER_HEARTBEAT == msg_type; end
        def self.worker_request?(msg_type) WORKER_REQUEST == msg_type; end
        def self.worker_reply_success?(msg_type) WORKER_REPLY_SUCCESS == msg_type; end
        def self.worker_reply_failure?(msg_type) WORKER_REPLY_FAILURE == msg_type; end
        def self.worker_disconnect?(msg_type) WORKER_DISCONNECT == msg_type; end
        
      end # class Message

    end
  end
end
