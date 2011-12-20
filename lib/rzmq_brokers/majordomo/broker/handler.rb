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
            @reactor.log(:info, "#{@klass_name}, Adding client request.")
            # send request to workers
            service.add_request(message)

          else
            # tell client the request failed
            if !service.request_ok?(message)
              @reactor.log(:warn, "#{@klass_name}, Request was rejected; failed!")
            else
              @reactor.log(:warn, "#{@klass_name}, No service to handle request; failed!")
            end

            send_client_failure(message.envelope_msgs, message)
            message.close
          end
        end

        def process_worker_reply(message)
          worker_identity = message.envelope_identity
          @reactor.log(:debug, "#{@klass_name}, Received reply from worker [#{worker_identity}]")

          if service = @services.find_service_by_worker_identity(worker_identity)
            service.process_reply(message)
          else
            @reactor.log(:warn, "#{@klass_name}, Could not find service for worker [#{worker_identity}], dropping reply #{message.sequence_id.inspect}.")
            message.close
          end
        end
        
        
        private
        
        def configure_messages_classes(config)
          @base_msg_klass = config.base_msg_klass.const_get('BrokerMessage')
          parent = config.base_msg_klass
          @reply_failure_msg_klass = parent.const_get("BrokerReplyFailure")
          @reply_success_msg_klass = parent.const_get("BrokerReplySuccess")
          @heartbeat_msg_klass = parent.const_get("Heartbeat")
          @disconnect_msg_klass = parent.const_get("Disconnect")
        end
      end # class Handler

    end
  end
end
