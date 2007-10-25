-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's licence can be found 
-- in the LICENCE file.
--

module("alua", package.seeall)

require("alua.channel")
require("alua.timer")
require("alua.event")
require("alua.daemon")
require("alua.process")

-- Export some functions
create  = alua.daemon.create
send    = alua.process.send
link    = alua.process.link
close   = alua.process.close
connect = alua.process.connect
spawn   = alua.process.spawn

--
-- This function opens a connection with a daemon.
-- It creates a daemon if necessary.
--
function open(cfg, cb)
   -- Connect to the daemon
   if type(cfg) == "string" then
      alua.connect(cfg, cb)
   -- Create the daemon before to connect
   elseif type(cfg) == "table" then
      local reply = function(msg)
         if msg.status == "ok" then
            alua.connect(msg.daemon, cb)
         else
            if cb then
               cb(msg)
            end
         end
      end
      alua.create(cfg, reply)
   else
      if cb then
         alua.task.schedule(cb, {status = "error", 
             error = "invalid #1 argument"})
      end
   end
end

--
-- Wait for events.
--
function loop()
   while true do
      alua.timer.poll()
      alua.channel.poll()
   end
end
