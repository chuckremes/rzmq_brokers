$:.push File.expand_path("../../../lib", __FILE__)

require 'rubygems'
#require '../../lib/rzmq_brokers'
require 'rzmq_brokers'
require 'broker'
require 'worker'
require 'message'
require 'client'

master_context = ZMQ::Context.new
log_transport = "inproc://reactor_log"

logger_config = ZM::Configuration.new do
  context master_context
  name 'logger-server'
end

ZM::Reactor.new(logger_config).run do |reactor|
  log_config = ZM::Server::Configuration.new do
    endpoint log_transport
    bind true
    topic ''
    context master_context
    reactor reactor
  end

  log_config.extra = {:file => STDOUT}

  log_server = ZM::LogServer.new(log_config)
end

# time for the log_server to spin up
sleep 1

# spawn a broker to handle workers and clients
broker = RunBroker.new(master_context, log_transport)
broker.run

# spawn a few workers to handle the client(s)
workers = []
3.times do
  worker = DBWorker.new(master_context, log_transport)
  worker.run
  workers << worker
end

# give broker and workers a chance to spin up, connect,
# handshake, etc before slamming them with requests
sleep 0.5

clients = []
1.times do
  client = DBClient.new(master_context, log_transport)
  client.run
  clients << client
end

sleep 1 until clients.none? { |client| client.running? }
