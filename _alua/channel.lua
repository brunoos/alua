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

-- Functions for dealing with channels.

module("channel")

local socket = require("socket")
local event  = require("event")

--
-- Create a client channel.
--
function
channel.client(host, port, read, write, close, s)
	-- Dirty hack so we can reuse this function. Bad, bad Pedro.
	if not s then
		s, e = socket.connect(host, port)
		if not s then return nil, e end
	end

	-- Prepare callback functions for the read and write operations.
	local read_callback, write_callback

	read_callback = function (sock, context)
		local pattern = context.pattern
		local data = sock:receive(pattern)
		if not data then return event.del(sock) end
		read(sock, data)
	end

	if write then
		write_callback = function (sock, context) write(sock) end
	end

	-- Create the event, with the terminator function, if any.
	event.add(s, { read = read_callback, write = write_callback },
	    { terminator = close })

	-- Return the socket.
	return s
end

--
-- Create a server channel.
--
function
channel.server(port, read, write, conn, close)
	-- Bind the socket.
	local s, e = socket.bind("*", port)
	if not s then return nil, e end

	-- Prepare a special callback for the server.
	local callback = function (sock, context)
		local s = conn(sock)
		if s then
			-- New 'child' channel, handle it.
			client(nil, nil, read, write, close, s)
		end
	end

	-- Create an event for it.
	event.add(s, { read = callback }, { terminator = close })

	-- Return the socket.
	return s
end

--
-- Close a channel.
--
function
channel.close(sock)
	-- Just remove the event.
	event.del(sock)
end
 
--
-- Get the pattern being used for a given channel.
--
function
channel.getpattern(sock)
	local _, context = event.get(sock)
	return context.pattern
end

--
-- Set the pattern to be used for a given channel.
--
function
channel.setpattern(sock, pattern)
	local _, context = event.get(sock)
	context.pattern = pattern
end
