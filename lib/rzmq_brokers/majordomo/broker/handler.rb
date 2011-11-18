module RzmqBrokers
  module Majordomo
    module Broker

      # Adds support for handling incoming requests and outgoing replies for
      # the Majordomo protocol.
      #
      class Handler < RzmqBrokers::Broker::Handler
        def dispatch_client_work(message)
          service = @services.find_service_by_name(message.service_name)
          if service && service.ready? && service.request_ok?(message)
            @reactor.log(:info, "MajordomoBroker, adding client request.")
            # send request to workers
            service.add_request(message)

          else
            # tell client the request failed
            if !service.request_ok?(message)
              @reactor.log(:warn, "MajordomoBroker, request was rejected; failed!")
            else
              @reactor.log(:warn, "MajordomoBroker, no service to handle request; failed!")
            end

            send_client_failure(message.envelope_msgs, message)
          end
        end

        def process_worker_reply(message)
          worker_identity = message.envelope_identity
          @reactor.log(:debug, "MajordomoBroker, received reply from worker [#{worker_identity}], msg #{message.inspect}")

          if service = @services.find_service_by_worker_identity(worker_identity)
            service.process_reply(message)
          end
        end
        
        
        private
        
        def configure_messages_classes(config)
          @base_msg_klass = config.base_msg_klass.const_get('Message')
          parent = config.base_msg_klass
          @client_reply_failure_msg_klass = parent.const_get("ClientReplyFailure")
          @client_reply_success_msg_klass = parent.const_get("ClientReplySuccess")
          @worker_heartbeat_msg_klass = parent.const_get("WorkerHeartbeat")
          @worker_request_msg_klass = parent.const_get("WorkerRequest")
          @worker_disconnect_msg_klass = parent.const_get("WorkerDisconnect")
        end
      end # class Handler

    end
  end
end
