-- Copyright (c) 2005 Lab//, PUC-Rio
-- All rights reserved.

-- This file is part of ALua. As a consequence, to every excerpt of code
-- hereby obtained, the respective project's licence applies. Detailed
-- information regarding ALua's licence can be found in the LICENCE file.

-- Timer functions (optional).

timercnt = 0

local timer_tab = {}

-- Add a new timer.
function
timeradd(cmd, freq)
	if not luatimer then require("luatimer") end
	local t, e = luatimer.insertTimer(freq)
	if not t then return nil, e end
	timer_tab[t] = cmd
	timercnt = timercnt + 1
	return t
end

-- Remove a timer.
function
timerdel(t)
	luatimer.removeTimer(t)
	timer_tab[t] = nil
	timercnt = timercnt - 1
end

-- Poll for timers expirations.
function
timerpoll()
	local t = luatimer.timeout()
	local f = timer_tab[t]
	if f then f(t) end
end
