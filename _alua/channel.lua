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

module("_alua.channel")

require("socket")
require("_alua.event")

-- Create a client channel.
function _alua.channel.client(host, port, read, write, close, s)
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
		if not data then return _alua.event.del(sock) end
		read(sock, data)
	end
	if write then
		write_callback = function (sock, context) write(sock) end
	end
	-- Create the event, with the terminator function, if any.
	_alua.event.add(s, { read = read_callback, write = write_callback },
	    { terminator = close })
	-- Return the socket.
	return s
end

-- Create a server channel.
function _alua.channel.server(port, read, write, conn, close)
	-- Bind the socket.
	local s, e = socket.bind("*", port)
	if not s then return nil, e end
	-- Prepare a special callback for the server.
	local callback = function (sock, context)
		local s = conn(sock)
		if s then -- New 'child' channel, handle it.
			client(nil, nil, read, write, close, s)
		end
	end
	-- Create an event for it.
	_alua.event.add(s, { read = callback }, { terminator = close })
	-- Return the socket.
	return s
end

-- Close a channel.
function _alua.channel.close(sock)
	_alua.event.del(sock) -- Just remove the event
end
 
-- Get the pattern being used for a given channel.
function _alua.channel.getpattern(sock)
	local _, context = _alua.event.get(sock)
	return context.pattern
end

-- Set the pattern to be used for a given channel.
function _alua.channel.setpattern(sock, pattern)
	local _, context = _alua.event.get(sock)
	context.pattern = pattern
end
