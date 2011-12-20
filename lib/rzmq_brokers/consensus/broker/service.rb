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
      # A worker disconnect does not cause an immediate consensus failure. It is
      # processed but the disconnect is ignored in the context of the request.
      #
      class Service < RzmqBrokers::Broker::Service
        class Requests
          class Request
            def initialize(service, handler, client_request)
              @service = service
              @handler = handler
              
              # client_request is not owned by this class; take what we need
              # and let the caller deal with freeing it
              @return_address = client_request.envelope_msgs

              @replies = {}
              @service.each { |worker| @replies[worker.identity] = nil }
            end

            def save_reply(message)
              if message.failure_reply?
                @message = message
                @service.close_request(self)
              else
                save_payload(message)

                if satisfied?
                  # use the last received message as the zero-copy carrier to return the result
                  # all the way back to the client; replace its payload with the saved payload
                  @message = message
                  @message.payload = saved_payload
                  
                  send_client_reply_success
                  @service.close_request(self)
                else
                  # awaiting more replies; close last message
                  message.close
                end
              end
            end
            
            def save_payload(message)
              @replies[message.envelope_identity] = message.payload.map { |msg| msg.copy_out_string }
            end

            def satisfied?
              @replies.values.all? { |reply| reply }
            end

            def send_client_reply_success
              @handler.send_client_reply_success(@return_address, @message)
            end

            def send_client_reply_failure
              @handler.send_client_reply_failure(@return_address, @message)
            end

            def saved_payload
              @replies.values.flatten.map { |string| ZMQ::Message.new(string) }
            end

            def close
              send_client_reply_failure unless satisfied?
            end
            
            # Called when a Worker has disconnected from the Broker and no longer should be
            # considered part of the consensus. If that worker was already sent a request,
            # we just drop the worker from the reply list.
            def ignore_reply_from(worker)
              @replies.delete(worker.identity)
            end
          end # class Request


          # Requests class
          #
          include Enumerable

          def initialize(service, handler)
            @service = service
            @handler = handler

            @open = Hash.new # key is sequence_no
            @closed = SortedArray.new
          end

          def each
            @open.values.each { |request| yield(request) }
          end

          def ready?() @open.size.zero?; end

          def add(client_request_msg)
            @open[client_request_msg.sequence_id] = Request.new(@service, @handler, client_request_msg)
            @service.each do |worker|
              @handler.send_worker_request(worker, client_request_msg.dup)
            end
            client_request_msg.close
          end

          def open?(message)
            @open.has_key?(message.sequence_id)
          end

          def closed?(message)
            # no op
          end

          def duplicate?(message)
            open?(message)
          end

          def process_reply(message)
            if open?(message)
              request = @open[message.sequence_id]
              request.save_reply(message)
            else
              message.close
            end
          end

          def close(request)
            seq_no = @open.respond_to?(:key) ? @open.key(request) : @open.index(request)

            index = @closed.index(seq_no) - 1
            old_seq_id = @closed[index]

            if old_seq_id && old_seq_id[1] == seq_no[1]
              @closed.delete_at(index)
              @closed.direct_insert(index, seq_no)
            else
              # wasn't found, so the index is no good for a direct insert
              @closed.insert(seq_no)
            end            

            @open.clear # there is only ever 1 open at a time, so just clear it
            request.close
          end
          
          # Called when a worker has requested disconnection from the consensus.
          def remove_worker(worker, request)
            request.ignore_reply_from(worker)
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
          remove_from_open_requests(worker)
        end

        def request_ok?(message)
          !@requests.duplicate?(message)
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

        def remove_from_open_requests(worker)
          @requests.each { |request| @requests.remove_worker(worker, request) }
        end
      end # class Service

    end
  end
end

