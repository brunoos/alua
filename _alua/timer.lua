-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's licence can be found 
-- in the LICENCE file.
--

module("_alua.timer", package.seeall)

-- Test (and load) if LuaTimer is available
local succ --= pcall(require, "luatimer")

----
-- LuaTimer is not available, define fake functions
--
if not succ then

local function fake() 
   -- do nothing
end

add = fake
del = fake
poll = fake

----
-- LuaTimer is available, define the 'true' functions
--
else

-- Count and table of active timers.
local active = {}

-- Add a new timer.
function add(cmd, freq)
   if not cmd or not freq then 
      return nil 
   end
   local t, e = luatimer.insertTimer(freq)
   if not t then
      return nil, e
   end
   active[t] = cmd
   return t
end

-- Remove a timer.
function del(t)
   luatimer.removeTimer(t)
   active[t] = nil
end

-- Poll for expirations.
function poll()
   local tt = luatimer.timeoutAll() or {}
   for _, t in pairs(tt) do
      local f = active[t]
      if f then 
         f(t) 
      end
   end
end

end -- end of LuaTimer test
