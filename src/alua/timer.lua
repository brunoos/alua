-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's license can be found 
-- in the LICENSE file.
--

module("alua.timer", package.seeall)

-- Try to load the Luatimer package
if not pcall(require, "luatimer") then

-- Luatimer not found: define fake functions

local function fake()
   return nil, "Luatimer not found"
end

create = fake
cancel = fake
flush  = fake
poll   = fake

else
-- Luatimer found: define real functions

-- Save the commands for each timer.
local commands = { }
-- The timer poll.
local timers = luatimer.createpoll()

--
-- Create a new timer.
--
function create(freq, cmd)
   if not freq or not cmd then 
      return nil, "invalid arguments"
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

end  -- Luatimer require
