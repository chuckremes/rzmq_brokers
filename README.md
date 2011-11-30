Notes:

Make sure that Broker's Worker timeout is *always* larger than the largest
possible worker timeout. This avoids the condition where the broker expects
heartbeats more often than the worker is willing to provide them. So, 
Broker::Worker-heartbeat <= Worker-heartbeat.
