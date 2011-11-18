
module RzmqBrokers
  module Consensus
    module Messages


      class WorkerMessage < Message
        attr_reader :sequence_id

        def worker?() true; end

        def ready?() false; end
        def disconnect?() false; end
        def request?() false; end
        def success_reply?() false; end
        def failure_reply?() false; end

        def envelope_identity
          # use second-to-last message as the identity of the worker
          Message.strhex(@envelope[-2])
        end

        def protocol_version_msg
          ZMQ::Message.new(WORKER_PROTOCOL_VERSION)
        end
      end # class WorkerMessage


      # 0  - protocol header
      # 1  - message type
      # 2  - service name
      #
      class WorkerReady < WorkerMessage
        def self.from_network(frames, envelope)
          service_name = frames.at(2).copy_out_string
          new(service_name, envelope)
        end

        def initialize(service_name, envelope = nil)
          @service_name = service_name
          @envelope = envelope
        end

        def ready?() true; end

        def to_msgs
          super + [ready_msg, service_name_msg]
        end

        def ready_msg
          ZMQ::Message.new(WORKER_READY)
        end
      end # class WorkerReady


      # 0  - protocol header
      # 1  - message type
      # 2  - service name
      #
      class WorkerDisconnect < WorkerMessage
        def self.from_network(frames, envelope)
          service_name = frames.at(2).copy_out_string
          new(service_name, envelope)
        end

        def initialize(service_name, envelope = nil)
          @service_name = service_name
          @envelope = envelope
        end

        def disconnect?() true; end

        def to_msgs
          super + [disconnect_msg, service_name_msg]
        end

        def disconnect_msg
          ZMQ::Message.new(WORKER_DISCONNECT)
        end
      end # class WorkerDisconnect


      # 0  - protocol header
      # 1  - message type
      # 2  - desired heartbeat interval in milliseconds
      # 3  - desired max retries
      #
      class WorkerHeartbeat < WorkerMessage
        attr_reader :heartbeat_interval, :heartbeat_retries

        def self.from_network(frames, envelope)
          heartbeat_interval = heartbeat_interval_decoder(frames.at(2).copy_out_string)
          heartbeat_retries = heartbeat_retries_decoder(frames.at(3).copy_out_string)
          new(heartbeat_interval, heartbeat_retries, envelope)
        end

        def initialize(heartbeat_interval, heartbeat_retries, envelope = nil)
          @heartbeat_interval = heartbeat_interval
          @heartbeat_retries = heartbeat_retries
          @envelope = envelope
        end

        def heartbeat?() true; end

        def to_msgs
          super + [heartbeat_msg, heartbeat_interval_msg, heartbeat_retries_msg]
        end

        def heartbeat_msg
          ZMQ::Message.new(WORKER_HEARTBEAT)
        end

        def heartbeat_interval_msg
          ZMQ::Message.new(Message.heartbeat_interval_encoder(@heartbeat_interval))
        end

        def heartbeat_retries_msg
          ZMQ::Message.new(Message.heartbeat_retries_encoder(@heartbeat_retries))
        end
      end # class WorkerHeartbeat


      # 0  - protocol header
      # 1  - message type
      # 2  - sequence number (64-bit integer, big-endian)
      # 3+ - application frames
      #
      class WorkerRequest < WorkerMessage
        attr_reader :request_payload

        def self.from_network(frames, envelope)
          sequence_id = sequence_decoder(frames.at(2).copy_out_string)
          request_payload = []
          i = 0
          while frames.at(3 + i)
            request_payload << frames.at(3 + i).copy_out_string
            i += 1
          end

          new(sequence_id, request_payload, envelope)
        end

        def self.from_client_request(message)
          new(message.sequence_id, message.payload)
        end

        def initialize(sequence_id, request_payload, envelope = nil)
          @sequence_id = sequence_id
          @request_payload = request_payload
          @envelope = envelope
        end

        def request?() true; end

        def to_msgs
          super + [request_msg, sequence_id_msg] + request_payload_msgs
        end

        def request_msg
          ZMQ::Message.new(WORKER_REQUEST)
        end

        def request_payload_msgs
          @request_payload.map { |detail| ZMQ::Message.new(detail) }
        end
      end # class WorkerRequest


      # 0  - protocol header
      # 1  - message type
      # 2  - sequence number (64-bit integer, big-endian)
      # 3+ - application frames
      #
      class WorkerReplySuccess < WorkerMessage
        def self.from_network(frames, envelope)
          sequence_id = sequence_decoder(frames.at(2).copy_out_string)
          request_payload = []
          i = 0
          while frames.at(3 + i)
            request_payload << frames.at(3 + i).copy_out_string
            i += 1
          end

          new(sequence_id, request_payload, envelope)
        end

        def initialize(sequence_id, payload, envelope = nil)
          @sequence_id = sequence_id
          @payload = payload
          @envelope = envelope
        end

        def success_reply?() true; end

        def to_msgs
          super + [reply_msg, sequence_id_msg] + payload_msgs
        end

        def reply_msg
          ZMQ::Message.new(WORKER_REPLY_SUCCESS)
        end
      end # class WorkerReplySuccess


      # 0  - protocol header
      # 1  - message type
      # 2  - sequence number (64-bit integer, big-endian)
      #
      class WorkerReplyFailure < WorkerMessage
        def self.from_network(frames, envelope)
          sequence_id = sequence_decoder(frames.at(2).copy_out_string)

          new(sequence_id, envelope)
        end

        def initialize(sequence_id, envelope = nil)
          @sequence_id = sequence_id
          @envelope = envelope
        end

        def failure_reply?() true; end

        def to_msgs
          super + [reply_msg, sequence_id_msg]
        end

        def reply_msg
          ZMQ::Message.new(WORKER_REPLY_FAILURE)
        end
      end # class WorkerReplyFailure

    end
  end
end
