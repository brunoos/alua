-- $Id$
-- copyright (c) 2005 pedro martelletto <pedro@ambientworks.net>
-- all rights reserved. part of the alua project.

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
	if not luatimer then return end
	luatimer.removeTimer(t)
	_alua.timer.active_table[t] = nil
	_alua.timer.active_count = _alua.timer.active_count - 1
end

-- Poll for expirations.
function _alua.timer.poll()
	if not luatimer then return end
	local tt = luatimer.timeoutAll() or {}
	for _, t in pairs(tt) do
		local f = _alua.timer.active_table[t]
		if f then f(t) end
	end
end
