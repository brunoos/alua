-- $Id$
--
-- Copyright (c) 2005 Pedro Martelletto <pedro@ambientworks.net>
-- All rights reserved.
--
-- This file is part of the ALua Project.
--
-- As a consequence, to every excerpt of code hereby obtained, the respective
-- project's licence applies. Detailed information regarding the licence used
-- in ALua can be found in the LICENCE file provided with this distribution.

-- This file implements a simple abstraction layer to parse incoming and
-- prepare outgoing data, according to the protocol used in ALua.

-- A small and generic event abstraction layer used by both processes and
-- daemons. Every event has its own context and an associated handler.

module("_alua.event")

require("socket") -- External modules

local event_panel = {}
local read_table  = {}
local write_table = {}

-- Auxiliar functions for inserting and removing an element from a list.

local tmp = {}

local function list_insert(list, element)
	table.insert(list, element); tmp[list] = tmp[list] or {}
	tmp[list][element] = table.getn(list)
end

local function list_remove(list, element)
	local index, last = tmp[list][element], table.remove(list)
	tmp[list][element] = nil; if last == element then return end
	list[index] = last; tmp[list][last] = index
end

-- Flush the event table. Used by the daemon when forking a new process.
function _alua.event.flush()
	for s in ipairs(event_panel) do s:close() end
	read_table, write_table, event_panel  = {}, {}, {}
end

-- Add a new event.
function _alua.event.add(sock, callbacks, context)
	-- Create and save the new event object.
	if callbacks.read  then list_insert(read_table, sock)  end
	if callbacks.write then list_insert(write_table, sock) end
	event_panel[sock] = { handlers = callbacks, context = context or {} }
end

-- Delete an event.
function _alua.event.del(sock)
	-- If the event has a terminator, execute it.
	local context = event_panel[sock].context
	if context.terminator then context.terminator(sock, context) end
	-- Remove the associated event object.
	local callbacks = event_panel[sock].handlers
	if callbacks.read then list_remove(read_table, sock) end
	if callbacks.write then list_remove(write_table, sock) end
	event_panel[sock] = nil; sock:close()
end

-- Get the handler and the context of a socket.
function _alua.event.get(sock)
	return event_panel[sock].handlers, event_panel[sock].context
end

function _alua.event.loop()
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
