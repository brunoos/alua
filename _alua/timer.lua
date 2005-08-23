-- $Id$
--
-- Copyright (c) 2005 Pedro Martelletto <pedro@ambientworks.net>
-- All rights reserved.
--
-- This file is part of the Alua Project.
--
-- As a consequence, to every excerpt of code hereby obtained, the respective
-- project's licence applies. Detailed information regarding the licence used
-- in Alua can be found in the LICENCE file provided with this distribution.

-- Timer support. Use LuaTimer to do the hard job, bind an API to Alua.

module("timer")

-- Count and table of active timers.
active_count, active_table = 0, {}

--
-- Add a new timer.
--
function
timer.add(cmd, freq)
	if not luatimer then require("luatimer") end
	local t, e = luatimer.insertTimer(freq)
	if not t then return nil, e end
	active_table[t] = cmd
	active_count = active_count + 1
	return t
end

--
-- Remove a timer.
--
function
timer.del(t)
	luatimer.removeTimer(t)
	active_table[t] = nil
	active_count = active_count - 1
end

--
-- Poll for expirations.
--
function
timer.poll()
	local t = luatimer.timeout()
	local f = active_table[t]
	if f then f(t) end
end
