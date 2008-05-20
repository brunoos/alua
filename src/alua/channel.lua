-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's license can be found 
-- in the LICENSE file.
--

module("alua.channel", package.seeall)

require("alua.config")
require("alua.channel.tcp")
require("alua.channel.mailbox")
require("alua.channel.mempair")

-- Alias
local cfgtimeout = alua.config.timeout

-- Constructors
local constructors = {
   ["tcp:server"]  = tcp.server,
   ["tcp:client"]  = tcp.client,
   ["mailbox"]     = mailbox.create,
   ["mailbox:out"] = mailbox.outgoing,
   ["mempair"]     = mempair.create,
}

--
-- Create a new channel.
--
function create(type, config, hdls)
   local func = constructors[type]
   assert(func, "invalid channel type")
   return func(type, config, hdls)
end

--
-- Poll the channels looking for new events.
--
function poll()
   local timeout = cfgtimeout
   if mailbox.poll() > 0 then
      timeout = 0
   end
   if mempair.poll() > 0 then
      timeout = 0
   end
   tcp.poll(timeout)
end
