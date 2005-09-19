-- $Id$

-- Copyright (c) 2005 Lab//, PUC-Rio
-- All rights reserved.

-- This file is part of ALua. As a consequence, to every excerpt of code
-- hereby obtained, the respective project's licence applies. Detailed
-- information regarding ALua's licence can be found in the LICENCE file.

-- Message routines for the ALua daemon.
module("_alua.daemon.message")

-- Encapsulation of external modules.
require("_alua.netio")

-- Auxiliary function for delivering a message to a process.
local function msg_delivery(context, dest, header, msg, callback)
	-- Look up the destination process in all
	-- the applications the sender is in.
	if context.command_table == _alua.daemon.command_table then
		context.apptable = _alua.daemon.apptable
	end
	for _, app in context.apptable do
		local socket = app.ptab[dest]
		if socket then -- Send the header, then the message.
			_alua.netio.async(socket, "message", header, callback)
			socket:send(msg)
		end
	end
end

-- Receive a message from a process and deliver it.
local function message_common(sock, context, header, reply, forwarding)
	-- Read in the message.
	local msg, e = sock:receive(header.len)
	if not header.from then header.from = context.id end
	-- Attempt to send the messge to each of the requested processes,
	-- filling the reply table accordingly.
	if type(header.to) == "table" and not forwarding then
		for _, dest in header.to do
			msg_delivery(context, dest, header, msg, reply)
		end
	else msg_delivery(context, header.to, header, msg, reply) end
end

-- Receive a message from a process and deliver it.
function _alua.daemon.message.from_process(sock, context, header, reply)
	local done, _reply = {}, {}
	local reply_callback = function (__reply)
		_reply[__reply.to] = { status = __reply.status,
		    error = __reply.error, to = __reply.to }
		if type(header.to) == "table" then
			table.insert(done, __reply.to)
			if table.getn(done) == table.getn(header.to) then
				reply(_reply) end -- Time to reply
		else reply(_reply) end -- Reply straight away
	end
	message_common(sock, context, header, reply_callback, false)
end

function _alua.daemon.message.from_daemon(sock, context, header, reply)
	local reply_callback = function (__reply) reply(__reply) end
	message_common(sock, context, header, reply_callback, true)
end
