# Majordomo example

Client makes a request of a db-lookup service looking for a 
record. Broker will pass the request to a registered Worker.
The Worker is set to take a random amount of time between 5
and 500 milliseconds to service the request.

Example illustrates a simple real-world use for this broker type.
It also shows how to build upon the Majordomo Message objects
to include application-specific payloads.

This example implements timeouts from
the client side. Each request is tagged with a timeout value in
milliseconds and maximum number of retries to attempt.
Failure to receive a successful reply within
that timeout period causes the client to automatically resend
the request. The hope is that the Broker will seed the request
to a less-busy Worker. If the request reaches its maximum retries
then it triggers the #on_failure method in the client.

The timeout retries and broker retries are hidden behind the
client interface. The code wrapping the client only needs to
provide two methods, #on_success and #on_failure. The client
will call one of those two methods when the request has been
satisfied, failed or timed out.

  ruby db_lookup.rb
  
# Client Details

blah

# Worker Details

blah

# Custom Message Details

blah
