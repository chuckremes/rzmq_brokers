# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "rzmq_brokers/version"

Gem::Specification.new do |s|
  s.name        = "rzmq_brokers"
  s.version     = RzmqBrokers::VERSION
  s.authors     = ["Chuck Remes"]
  s.email       = ["cremes@mac.com"]
  s.homepage    = ""
  s.summary     = %q{A small toolkit for writing 0mq-based brokers and custom protocols}
  s.description = %q{This is a toolkit for writing 0mq-based brokers that use custom
    protocols. It comes with two protocols, namely Consensus and (modified) Majordomo. An
    examples directory describes how to use to toolkit to build your own custom clients,
    workers, and messages.}

  s.rubyforge_project = "rzmq_brokers"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  # specify any dependencies here; for example:
  # s.add_development_dependency "rspec"
  s.add_runtime_dependency "zmqmachine", ">= 0.7.0"
end
