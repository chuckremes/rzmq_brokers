module RzmqBrokers
  module Majordomo
    module Broker

      # Used by the Broker to track the state of a Service, its Workers and any
      # open Requests made by Clients.
      #
      # Enforces the logic of the majordomo protocol.
      #
      class Service < RzmqBrokers::Broker::Service
        class Requests
          class Request
            def initialize(service, handler, client_request, worker)
              @service = service
              @handler = handler
              @client_request = client_request
              @worker = worker

              @reply = nil
            end

            def save_reply(message)
              @reply = message
              @service.close_request(self)
            end

            def satisfied?
              if @reply
                !@reply.failure_reply?
              else
                false
              end
            end

            def send_client_reply_success
              @handler.send_client_reply_success(@client_request.envelope_msgs, @service.name, @client_request.sequence_id, saved_payload)
            end

            def send_client_reply_failure
              @handler.send_client_reply_failure(@client_request.envelope_msgs, @service.name, @client_request.sequence_id, saved_payload)
            end

            def saved_payload
              @reply.payload if @reply
            end

            def close
              satisfied? ? send_client_reply_success : send_client_reply_failure
              
              @service.add_mru_worker(@worker)
            end

            def assigned_this_worker?(worker)
              worker == @worker
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
            @queue = Array.new
          end

          def each
            @open.values.each { |request| yield(request) }
          end

          def ready?() true; end

          def add(client_request)
            @queue << client_request
            process_requests
          end

          def process_requests
            if @service.available_worker? && available_request?
              worker = @service.get_lru_worker
              client_request = @queue.shift

              @open[client_request.sequence_id] = Request.new(@service, @handler, client_request, worker)
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
              request = @open[message.sequence_id]
              request.save_reply(message)
            end

            # a worker may have just become available, so process the next request
            process_requests
          end

          def close(request)
            seq_no = @open.respond_to?(:key) ? @open.key(request) : @open.index(request)
            @closed << seq_no
            @open.delete(seq_no)
            request.close
          end

          # Closes the request associated with the given +worker+.
          def fail_for_worker(worker)
            # ask each request to close itself if the given worker is the one
            # assigned to the request
            each do |request|
              @service.close_request(request) if request.assigned_this_worker?(worker)
            end
          end


          private

          def available_request?
            @queue.size > 0
          end
        end # class Requests


        # Service class
        #
        def initialize(*args)
          super
          @requests = Requests.new(self, @handler)
          @ordered_workers = Array.new
        end

        # Adds worker to the service *and* appends it to the ordered worker list.
        #
        def add(worker)
          super
          @ordered_workers.unshift(worker)
        end

        # Removes the worker from the service *and* deletes it from the ordered
        # worker list.
        #
        def delete(worker)
          super
          @ordered_workers.delete(worker)
          fail_open_requests(worker)
        end

        # Confirms that this request is allowed. Some brokers may want to reject
        # duplicate requests (problematic if the client retries after a timeout),
        # reject requests that were already closed, etc. For now, this just okay's
        # everything.
        #
        def request_ok?(message)
          true
        end

        # Enqueues this as an open request and gives this task to the least
        # recently used (LRU) worker when one becomes available.
        #
        def add_request(message)
          @requests.add(message)
        end

        def close_request(request)
          @requests.close(request)
        end

        def process_reply(message)
          worker = super
          @requests.process_reply(message)
        end

        # A timed-out or disconnected worker should
        # cause the request associated with the deleted worker to fail.
        def fail_open_requests(worker)
          # need to fail the request associated with this worker
          @requests.fail_for_worker(worker)
        end

        def available_worker?
          @ordered_workers.size > 0
        end

        # Get the least-recently used (i.e. first) worker from the ordered array.
        def get_lru_worker
          @ordered_workers.shift
        end

        # Add to the end of the Array. Array is ordered least-recently used to most-recently
        # used.
        def add_mru_worker(worker)
          @ordered_workers << worker
        end
      end # class Service

    end
  end
end
