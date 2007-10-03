-- $Id: timer.lua 129 2007-05-12 19:27:34Z brunoos $
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's licence can be found 
-- in the LICENCE file.
--

module("alua.timer", package.seeall)

require("luatimer")

-- Save the commands for each timer.
local commands = { }
-- The timer poll.
local timers = luatimer.createpoll()

--
-- Create a new timer.
--
function create(freq, cmd)
   if not freq or not cmd then 
      return nil 
   end
   local t = timers:create(freq)
   commands[t] = cmd
   return t
end

--
-- Cancel a timer.
--
function cancel(t)
   commands[t] = nil
   timers:cancel(t)
end

--
-- Remove all timers.
--
function flush()
   commands = {}
   timers:cancel("all")
end

--
-- Poll for expirations.
--
function poll()
   for _, t in ipairs(timers:fired("all")) do
      local cmd = commands[t]
      if cmd then 
         cmd(t) 
      end
   end
end
