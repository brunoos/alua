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

module("_alua.timer")

-- Count and table of active timers.
_alua.timer.active_count, _alua.timer.active_table = 0, {}

-- Add a new timer.
function _alua.timer.add(cmd, freq)
	if not cmd or not freq then return nil end
	if not luatimer then require("luatimer") end
	local t, e = luatimer.insertTimer(freq)
	if not t then return nil, e end
	_alua.timer.active_table[t] = cmd
	_alua.timer.active_count = _alua.timer.active_count + 1
	return t
end

-- Remove a timer.
function _alua.timer.del(t)
	luatimer.removeTimer(t)
	_alua.timer.active_table[t] = nil
	_alua.timer.active_count = _alua.timer.active_count - 1
end

-- Poll for expirations.
function _alua.timer.poll()
	local tt = luatimer.timeoutAll()
	for _, t in tt do
		local f = _alua.timer.active_table[t]
		if f then f(t) end
	end
end
