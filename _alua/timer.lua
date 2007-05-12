-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's licence can be found 
-- in the LICENCE file.
--

module("_alua.timer", package.seeall)

-- Count and table of active timers.
active_count = 0
active_table = {}

-- Add a new timer.
function add(cmd, freq)
   if not cmd or not freq then 
      return nil 
   end
   if not luatimer then 
      require("luatimer") 
   end
   local t, e = luatimer.insertTimer(freq)
   if not t then
      return nil, e
   end
   active_table[t] = cmd
   active_count = active_count + 1
   return t
end

-- Remove a timer.
function del(t)
   if not luatimer then 
      return 
   end
   luatimer.removeTimer(t)
   active_table[t] = nil
   active_count = active_count - 1
end

-- Poll for expirations.
function poll()
   if not luatimer then 
      return 
   end
   local tt = luatimer.timeoutAll() or {}
   for _, t in pairs(tt) do
      local f = active_table[t]
      if f then 
         f(t) 
      end
   end
end
