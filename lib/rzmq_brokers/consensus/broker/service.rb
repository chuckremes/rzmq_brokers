module RzmqBrokers
  module Consensus
    module Broker

      # Used by the Broker to track the state of a Service, its Workers and any
      # open Requests made by Clients.
      #
      # Enforces the logic of a consensus. All attached Workers must reply with
      # a success message before the Request is satisifed and a success response
      # is sent to the originating Client. Any worker timeouts or failure replies
      # cause the entire Request to fail.
      #
      class Service < RzmqBrokers::Broker::Service
        class Requests
          class Request
            def initialize(service, handler, client_request)
              @service = service
              @handler = handler
              @client_request = client_request

              @replies = {}
              @service.each { |worker| @replies[worker.identity] = nil }
            end

            def duplicate_reply?(message)
              # true when we have already recorded a reply
              @replies[message.envelope_identity]
            end

            def save_reply(message)
              if message.failure_reply?
                @service.close_request(self)
              else
                @replies[message.envelope_identity] = message.payload

                if satisfied?
                  send_client_reply_success
                  @service.close_request(self)
                end
              end
            end

            def satisfied?
              @replies.values.all? { |reply| reply }
            end

            def send_client_reply_success
              @handler.send_client_reply_success(@client_request.envelope_msgs, @service.name, @client_request.sequence_id, saved_payload)
            end

            def send_client_reply_failure
              @handler.send_client_reply_failure(@client_request.envelope_msgs, @service.name, @client_request.sequence_id)
            end

            def saved_payload
              @replies.values.flatten
            end

            def close
              send_client_reply_failure unless satisfied?
            end
          end # class Request


          # Requests class
          #
          include Enumerable

          def initialize(service, handler)
            @service = service
            @handler = handler

            @open = Hash.new # key is sequence_no
            @closed = Array.new
          end

          def each
            @open.values.each { |request| yield(request) }
          end

          def ready?() @open.size.zero?; end

          def add(client_request)
            @open[client_request.sequence_id] = Request.new(@service, @handler, client_request)
            @service.each do |worker|
              @handler.send_worker_request(worker, client_request)
            end
          end

          def open?(message)
            @open.has_key?(message.sequence_id)
          end

          def closed?(message)
            @closed.include?(message.sequence_id)
          end

          def duplicate?(message)
            open?(message) || closed?(message)
          end

          def process_reply(message)
            if open?(message)
              unless (request = @open[message.sequence_id]).duplicate_reply?(message)
                request.save_reply(message)
              end
            end
          end

          def close(request)
            seq_no = @open.respond_to?(:key) ? @open.key(request) : @open.index(request)
            @closed << seq_no
            @open.delete(seq_no)
            request.close
          end
        end # class Requests


        # Service class
        #
        def initialize(*args)
          super
          @requests = Requests.new(self, @handler)
        end
        
        def delete(worker)
          super
          fail_open_requests
        end

        def request_ok?(message)
          !@requests.duplicate?(message) && !@requests.closed?(message)
        end

        def ready?
          @requests.ready?
        end

        # Saves this as an open request and informs all workers of the new task.
        #
        def add_request(message)
          @requests.add(message)
        end

        def close_request(request)
          @requests.close(request)
        end

        def process_reply(message)
          super
          @requests.process_reply(message)
        end

        def fail_open_requests
          @requests.each { |request| close_request(request) }
        end
      end # class Service

    end
  end
end

