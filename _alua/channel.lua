-- $Id$
-- copyright (c) 2005 pedro martelletto <pedro@ambientworks.net>
-- all rights reserved. part of the alua project.

module("_alua.channel")

require("socket") -- external modules
require("_alua.event") -- internal modules

-- create a client channel
function _alua.channel.client(host, port, read, write, close, s)
	-- dirty hack so we can reuse this function. bad, bad pedro...
	if not s then
		s, e = socket.connect(host, port)
		if not s then return nil, e end
	end
	local read_callback, write_callback
	read_callback = function (sock, context)
		local pattern, data = context.pattern, sock:receive(pattern)
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

-- create a server channel
function _alua.channel.server(port, read, write, conn, close)
	local s, e = socket.bind("*", port)
	if not s then return nil, e end
	local callback = function (sock, context) -- prepare special callback
		local s = conn(sock)
		if s then -- new 'child' channel, handle it
			client(nil, nil, read, write, close, s) end
	end; _alua.event.add(s, { read = callback }, { terminator = close })
	return s
end

-- close a channel
function _alua.channel.close(sock)
	_alua.event.del(sock) -- just remove the event
end
 
-- get the pattern being used for a given channel
function _alua.channel.getpattern(sock)
	local _, context = _alua.event.get(sock); return context.pattern
end

-- set the pattern to be used for a given channel
function _alua.channel.setpattern(sock, pattern)
	local _, context = _alua.event.get(sock); context.pattern = pattern
end
