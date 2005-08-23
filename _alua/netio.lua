-- $Id$

-- Copyright (c) 2005 Lab//, PUC-Rio
-- All rights reserved.

-- This file is part of ALua. As a consequence, to every excerpt of code
-- hereby obtained, the respective project's licence applies. Detailed
-- information regarding ALua's licence can be found in the LICENCE file.

-- A simple abstraction layer to parse incoming and prepare outgoing data.
module("netio")

-- Encapsulation of external modules.
local event = require("event")
local utils = require("utils")

-- Transform an outgoing '<command>, <argument table>' pair into a string and
-- send it on the given socket.
function
send(sock, cmd, arg)
	sock:send(string.format("%s %s\n", cmd, utils.dump(arg)))
end

-- Receive and parse incoming data into a '<command>, <argument table>' pair.
function
recv(sock)
	-- Receive the packet.
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

-- Generic event handling function. Takes care of the sequence count.
function
handler(sock, context)
	-- Receive and parse the message.
	local cmd, data, e = recv(sock)

	-- In case of network error, remove the associated event. Specific
	-- steps regarding the socket's decomissioning can be taken in the
	-- terminator function called by the event module.
	if not data then event.del(sock) return end
	-- In case of parse error, return an error message.
	if not cmd then send(sock, "error", { value = e }) return end

	-- If the packet is a reply for a request we made, do the callback.
	if string.find(cmd, "-reply$") then
		local callback = context.seqtable[data.seqcount]
		context.seqtable[data.seqcount] = nil
		data.seqcount = nil
		if callback then callback(data) end
		-- If this was our topmost sequence number, decrement it.
		if data.seqcount == context.seqcount - 1 then
			context.seqcount = data.seqcount - 1
		end
	else
		-- Otherwise, the packet holds an incoming request. Try to find
		-- a handler for it and return the reply, if any.
		if not context.cmdtab[cmd] then
			send(sock, "error", { value = "Invalid command" })
		else
			local rep = context.cmdtab[cmd](sock, context, data)
			if rep then
				-- Send the reply, and keep state.
				rep.seqcount = data.seqcount
				send(sock, cmd .. "-reply", rep)
			end
		end
	end
end

-- Issue a protocol command, asynchronously.
function
cmd(sock, cmd, arg, callback)
	-- Get the socket context.
	local _, context = event.get(sock)

	-- Arrange the socket's sequence record.
	context.seqcount = context.seqcount or 0
	context.seqtable = context.seqtable or {}

	-- Tag the request with the current seqcount, so that we can identify
	-- the reply when it arrives (as the channel could be shared).
	arg.seqcount = context.seqcount
	context.seqtable[arg.seqcount] = callback
	context.seqcount = context.seqcount + 1

	send(sock, cmd, arg)
end

-- Reply to a protocol command.
function
reply(sock, cmd, oldp, newp)
	newp.seqcount = oldp.seqcount
	send(sock, cmd .. "-reply", newp)
end
