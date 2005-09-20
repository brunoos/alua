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

-- This file implements a simple abstraction layer to parse incoming and
-- prepare outgoing data, according to the protocol used in Alua.

module("_alua.netio")

require("_alua.event"); require("_alua.utils") -- Internal modules

-- Transform an outgoing '<mode>, <header>, <arguments>' tuple into a string
-- and send it on the given socket.
function _alua.netio.send(sock, mode, header, data)
	sock:send(string.format("%s = %s, arguments = %s\n", mode,
	    _alua.utils.dump(header), _alua.utils.dump(data)))
end

-- Receive and parse incoming data into a '<command>, <arguments>' pair.
function _alua.netio.recv(sock)
	-- Read the packet from the socket.
	local data, e = sock:receive(); if not data then return nil end
	data = "return { " .. data .. " }" -- Load the received chunk
	local f, e = loadstring(data); if not f then return nil end
	setfenv(f, {}); return f()
end

-- Handle an incoming request. Try to find a handler for it, and prepare a
-- reply function to be used by the handler. If a timeout was given, set up
-- a timer for it.
function _alua.netio.request(sock, context, incoming)
	local request, arguments = incoming.request, incoming.arguments
	local timer, timer_expired
	local reply = function (body)
		if timer_expired then return end
		_alua.netio.send(sock,  "reply", { id = request.id }, body)
		if timer then _alua.timer.del(timer) end
	end
	local timer_callback = function(t)
		reply({ status = "error", error = "timeout" })
		timer_expired = true -- Nullify reply()
		_alua.timer.del(t)
	end
	timer = _alua.timer.add(timer_callback, request.timeout)
	local handler = context.command_table[request.name]
	handler(sock, context, arguments, reply)
end

-- Handle an incoming reply.
function _alua.netio.reply(sock, context, incoming)
	local reply, arguments = incoming.reply, incoming.arguments
	-- Try to find a callback we should call.
	local callback = context.reqtable[reply.id]
	-- We have a match, remove the entry from the sequence table.
	context.reqtable[reply.id] = nil
	-- If this was our topmost sequence number, decrement it.
	if reply.id == context.reqcount - 1 then
		context.reqcount = reply.id - 1
	end; if callback then callback(arguments) end
end

-- Generic event handling function. Takes care of the sequence count, which is
-- needed since we can share multiple instances of asynchronous requests in a
-- same channel.
function _alua.netio.handler(sock, context)
	local incoming = _alua.netio.recv(sock) or {}
	if incoming.request then _alua.netio.request(sock, context, incoming)
	elseif incoming.reply then _alua.netio.reply(sock, context, incoming)
	else _alua.event.del(sock) end
end

-- Issue a protocol command, asynchronously.
function _alua.netio.async(sock, cmd, arg, callback, timeout)
	-- Get the socket context.
	local _, context = _alua.event.get(sock)
	-- Arrange the socket's sequence record.
	context.reqcount = context.reqcount or 0
	context.reqtable = context.reqtable or {}
	-- Tag the request with the current sequence count, so that we can
	-- identify the reply when it arrives (as the channel could be shared).
	local req = { name = cmd, timeout = timeout, id = context.reqcount }
	context.reqtable[context.reqcount] = callback
	context.reqcount = context.reqcount + 1
	_alua.netio.send(sock, "request", req, arg)
end

-- Issue a protocol command, synchronously.
function _alua.netio.sync(sock, cmd, args)
	_alua.netio.send(sock, "request", { name = cmd }, args)
	return _alua.netio.recv(sock)
end
