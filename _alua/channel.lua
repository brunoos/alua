-- $Id$
-- copyright (c) 2005 pedro martelletto <pedro@ambientworks.net>
-- all rights reserved. part of the alua project.

module("_alua.channel")

require("socket") -- External modules
require("_alua.event") -- Internal modules

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
	_alua.event.add(s, { read = read_callback, write = write_callback },
	    { terminator = close })
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
			client(nil, nil, read, write, close, s) end
	end; _alua.event.add(s, { read = callback }, { terminator = close })
	return s
end

-- Close a channel.
function _alua.channel.close(sock)
	_alua.event.del(sock) -- Just remove the event
end
 
-- Get the pattern being used for a given channel.
function _alua.channel.getpattern(sock)
	local _, context = _alua.event.get(sock); return context.pattern
end

-- Set the pattern to be used for a given channel.
function _alua.channel.setpattern(sock, pattern)
	local _, context = _alua.event.get(sock); context.pattern = pattern
end
