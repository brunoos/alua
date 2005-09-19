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

require("_alua.event")
require("_alua.utils")

--
-- Transform an outgoing '<command>, <argument table>' pair into a string and
-- send it on the given socket.
--
function
_alua.netio.send(sock, cmd, arg)
	sock:send(string.format("%s %s\n", cmd, _alua.utils.dump(arg)))
end

--
-- Receive and parse incoming data into a '<command>, <argument table>' pair.
--
function
_alua.netio.recv(sock)
	-- Read the packet from the socket.
	local data, e = sock:receive()
	if not data then return nil, nil, e end

	-- Extract the first token.
	local i, e, cmd = string.find(data, "^([-%a]+)")
	if not i then return nil, data, "Invalid command" end

	-- Chop it off the packet.
	data = string.sub(data, e + 2)
	data = "return " .. data

	-- And safely load the chunk passed.
	local f, e = loadstring(data)
	if not f then return nil, data, e end
	setfenv(f, {}); local arg, e = f()
	if not arg then return nil, data, e end

	return cmd, arg
end

-- Handle an incoming request. Try to find a handler for it, and prepare a
-- reply function to be used by the handler.
function _alua.netio.request(sock, context, cmd, data)
	local handler = context.command_table[cmd]
	if not handler then
		_alua.netio.send(sock, "error", { value = "Invalid command" } )
		return
	end
	local reply = function (body)
		body.sequence_count = data.sequence_count -- Keep state.
		_alua.netio.send(sock, cmd .. "-reply", body)
	end
	handler(sock, context, data, reply)
end

--
-- Handle an incoming reply.
--
function
_alua.netio.reply(context, data)
	-- Try to find a callback we should call.
	local callback = context.sequence_table[data.sequence_count]

	-- If there's no callback, then something might be wrong.
	if not callback then
--		print("Warning, asynchronous reply with no callback?")
		return
	end

	-- We have a match, remove the entry from the sequence table.
	context.sequence_table[data.sequence_count] = nil

	-- If this was our topmost sequence number, decrement it.
	if data.sequence_count == context.sequence_count - 1 then
		context.sequence_count = data.sequence_count - 1
	end

	-- Hide sequence number from callback.
	data.sequence_count = nil

	-- Do it, finally.
	callback(data)
end

--
-- Generic event handling function. Takes care of the sequence count, which is
-- needed since we can share multiple instances of asynchronous requests in a
-- same channel.
--
function
_alua.netio.handler(sock, context)
	-- Receive and parse the message.
	local cmd, data, e = _alua.netio.recv(sock)

	-- In case of network error, remove the associated event.
	if not data then _alua.event.del(sock) return end

	-- In case of parse error, return an error message.
	if not cmd then _alua.netio.send(sock, "error", { value = e } ) return end

	-- Check if the packet is a reply for a request we made.
	if string.find(cmd, "-reply$") then
		_alua.netio.reply(context, data)
	else
		-- Otherwise, the packet holds an incoming request.
		_alua.netio.request(sock, context, cmd, data)
	end
end

--
-- Issue a protocol command, asynchronously.
--
function
_alua.netio.cmd(sock, cmd, arg, callback)
	-- Get the socket context.
	local _, context = _alua.event.get(sock)

	-- Arrange the socket's sequence record.
	context.sequence_count = context.sequence_count or 0
	context.sequence_table = context.sequence_table or {}

	-- Tag the request with the current sequence count, so that we can
	-- identify the reply when it arrives (as the channel could be shared).
	arg.sequence_count = context.sequence_count
	context.sequence_table[arg.sequence_count] = callback
	context.sequence_count = context.sequence_count + 1

	_alua.netio.send(sock, cmd, arg)
end

-- Issue a protocol command, synchronously.
function _alua.netio.sync(sock, cmd, args)
	local reply_cmd, reply, e
	_alua.netio.send(sock, cmd, args)
	reply_cmd, reply, e = _alua.netio.recv(sock)
	if not reply_cmd or reply_cmd ~= cmd .. "-reply" then
		return nil, "Invalid reply for command"
	end
	return reply
end

--
-- The spawn algorithm needs to be reviewed, so this kludge can be removed.
--
function
_alua.netio.spawn_reply(sock, cmd, oldp, newp)
	newp.sequence_count = oldp.sequence_count
	_alua.netio.send(sock, cmd .. "-reply", newp)
end
