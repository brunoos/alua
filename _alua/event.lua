-- Copyright (c) 2005 Lab//, PUC-Rio
-- All rights reserved.

-- This file is part of ALua. As a consequence, to every excerpt of code
-- hereby obtained, the respective project's licence applies. Detailed
-- information regarding ALua's licence can be found in the LICENCE file.

-- A small and generic event abstraction layer used by both processes and
-- daemons. Every event has its own context and an associated handler.
module("event")

local socket = require("socket")
local event_panel = {}
local read_table  = {}
local write_table = {}

-- Flush the event table. Used by the daemon when forking a new process.
function
flush()
	for s in ipairs(event_panel) do s:close() end
	read_table  = {}
	write_table = {}
	event_panel = {}
end

-- Add a new event.
function
add(sock, callbacks, context)
	-- Create the new event object.
	if callbacks.read  then table.insert(read_table, sock)  end
	if callbacks.write then table.insert(write_table, sock) end

	event_panel[sock] = { handlers = callbacks, context = context or {} }
end

-- Delete an event.
function
del(sock)
	-- If the event has a terminator, execute it.
	local context = event_panel[sock].context
	if context.terminator then context.terminator(sock, context) end

	-- Remove the associated event object.
--	local idx = event_panel[sock].index
--	event_table[idx] = nil
--	event_panel[sock] = nil
	sock:close()
end

-- Get the handler and the context of a socket.
function
get(sock)
	return event_panel[sock].handlers, event_panel[sock].context
end

function
loop()
	-- Check for activity in the events sockets.
	local ractive, wactive = socket.select(read_table, write_table, 1)

	for _, s in ipairs(ractive) do
		-- Do the respective callbacks.
		local callback = event_panel[s].handlers.read
		if callback then callback(s, event_panel[s].context) end
	end

	for _, s in ipairs(wactive) do
		-- Do the respective callbacks.
		local callback = event_panel[s].handlers.write
		if callback then callback(s, event_panel[s].context) end
	end

	-- And return the number of events.
	return table.getn(read_table) + table.getn(write_table)
end
