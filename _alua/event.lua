-- $Id$

-- Copyright (c) 2005 Pedro Martelletto <pedro@ambientworks.net>
-- All rights reserved. Part of the ALua project.

module("_alua.event")

require("socket") -- external modules

local event_panel, read_table, write_table = {}, {}, {}

-- auxiliar functions for inserting and removing an element from a list

local tmp = {}

local function list_insert(list, element)
	table.insert(list, element)
	tmp[list] = tmp[list] or {}
	tmp[list][element] = table.getn(list)
end

local function list_remove(list, element)
	local index, last = tmp[list][element], table.remove(list)

	tmp[list][element] = nil
	
	if last == element then
		return
	end

	list[index] = last
	tmp[list][last] = index
end

-- flush event table
function _alua.event.flush()
	for s in ipairs(event_panel) do
		s:close()
	end

	read_table, write_table, event_panel  = {}, {}, {}
end

-- add a new event
function _alua.event.add(sock, callbacks, context)
	if callbacks.read then
		list_insert(read_table, sock)
	end

	if callbacks.write then
		list_insert(write_table, sock)
	end

	event_panel[sock] = { handlers = callbacks, context = context or {} }
end

-- delete an event
function _alua.event.del(sock)
	local context = event_panel[sock].context

	if context.terminator then
		context.terminator(sock, context)
	end

	local callbacks = event_panel[sock].handlers

	if callbacks.read then
		list_remove(read_table, sock)
	end

	if callbacks.write then
		list_remove(write_table, sock)
	end

	event_panel[sock] = nil
	sock:close()
end

-- get the handler and context of a socket
function _alua.event.get(sock)
	return event_panel[sock].handlers, event_panel[sock].context
end

function _alua.event.loop()
	-- check for activity in the events sockets
	local ractive, wactive = socket.select(read_table, write_table, 1)

	for _, s in ipairs(ractive) do
		-- do the respective callbacks
		local callback = event_panel[s].handlers.read

		if callback then
			callback(s, event_panel[s].context)
		end
	end

	for _, s in ipairs(wactive) do
		-- do the respective callbacks
		local callback = event_panel[s].handlers.write

		if callback then
			callback(s, event_panel[s].context)
		end
	end

	-- and return the number of events
	return table.getn(read_table) + table.getn(write_table)
end
