module RzmqBrokers
  module Broker

    class Services
      def initialize(reactor, handler, service_klass, worker_klass)
        @reactor = reactor
        @handler = handler
        @service_klass = service_klass
        @worker_klass = worker_klass

        @services = Hash.new # key 'k' is the service name
      end

      def register(service_name, identity, heartbeat_interval, heartbeat_retries, envelope)
        unless @services.has_key?(service_name)
          @reactor.log(:info, "#{self.class}, Creating service for [#{service_name}]")
          @services[service_name] = @service_klass.new(@reactor, service_name, @handler)
        end

        @services[service_name].add(@worker_klass.new(@reactor, @handler, service_name, identity, heartbeat_interval, heartbeat_retries, envelope))
      end

      # Deletes the named service and all of its workers.
      #
      def deregister(service_name)
        service = @services[service_name]

        if service
          service.each { |worker| service.delete(worker) }
          @services.delete(service_name)
        end
      end

      # Deletes the given worker from the service to which it belongs.
      #
      def deregister_worker(worker)
        service = @services[worker.service_name]
        service && service.delete(worker)
      end

      def find_worker(identity)
        found = false
        worker = nil
        @services.values.flatten.each do |service|
          break if worker = service[identity]
        end

        worker
      end

      def find_service_by_worker_identity(identity)
        @services.values.flatten.find { |service| service[identity] }
      end

      def find_service_by_name(name)
        @services[name]
      end

      def purge_expired_workers
        @services.values.each { |service| service.purge_expired_workers }
      end
    end # class Services

  end
end
