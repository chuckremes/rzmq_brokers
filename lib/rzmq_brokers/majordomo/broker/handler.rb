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
            @reactor.log(:info, "#{self.class}, Adding client request.")
            # send request to workers
            service.add_request(message)

          else
            # tell client the request failed
            if !service.request_ok?(message)
              @reactor.log(:warn, "#{self.class}, Request was rejected; failed!")
            else
              @reactor.log(:warn, "#{self.class}, No service to handle request; failed!")
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
          @reply_failure_msg_klass = parent.const_get("ReplyFailure")
          @reply_success_msg_klass = parent.const_get("ReplySuccess")
          @heartbeat_msg_klass = parent.const_get("Heartbeat")
          @request_msg_klass = parent.const_get("Request")
          @disconnect_msg_klass = parent.const_get("Disconnect")
        end
      end # class Handler

    end
  end
end
