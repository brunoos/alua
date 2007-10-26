-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's licence can be found 
-- in the LICENCE file.
--

-- Do not use 'package.seeall' because the fields 'alua.id' and 'alua.daemonid'
-- can be nil. This will trigger a search in the environment and it can break 
-- ALua. Instead, use a local variable to access the global environment.

local env = _G

module("alua")

env.require("alua.channel")
env.require("alua.timer")
env.require("alua.event")
env.require("alua.task")
env.require("alua.daemon")
env.require("alua.process")

-- Export some functions
create = daemon.create
send   = process.send
link   = process.link
close  = process.close
spawn  = process.spawn

--
-- This function opens a connection with a daemon.
-- It creates a daemon if necessary.
--
function open(cfg, cb)
   -- Connect to the daemon
   if env.type(cfg) == "string" then
      process.connect(cfg, cb)
   -- Create the daemon before to connect
   elseif type(cfg) == "table" then
      local reply = function(msg)
         if msg.status == "ok" then
            process.connect(msg.daemon, cb)
         else
            if cb then
               cb(msg)
            end
         end
      end
      create(cfg, reply)
   else
      if cb then
         task.schedule(cb, {status = "error", 
             error = "invalid #1 argument"})
      end
   end
end

--
-- Wait for events.
--
function loop()
   while true do
      timer.poll()
      channel.poll()
   end
end
