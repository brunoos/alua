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
--
-- Message routines for the ALua daemon.
module("_alua.daemon.message")

require("_alua.netio") -- External modules

-- Auxiliary function for delivering a message to a process.
local function msg_delivery(context, header, msg, callback)
	if context.command_table == _alua.daemon.command_table then
		context.apptable = _alua.daemon.apptable end
	for _, app in context.apptable do
		local socket = app.processes[header.to]
		if socket then  -- Send the header, then the message
			_alua.netio.async(socket, "message", header, callback)
			socket:send(msg) end
	end
end

-- Receive a message from a process and deliver it.
local function message_common(sock, context, header, reply, forwarding)
	local msg, e = sock:receive(header.len) -- Read in the message
	if not header.from then header.from = context.id end
	if type(header.to) == "table" and not forwarding then
		for _, dest in header.to do -- Fake new header
			local newheader = header; newheader.to = dest
			msg_delivery(context, header, msg, reply) end
	else msg_delivery(context, header, msg, reply) end
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
	end; message_common(sock, context, header, reply_callback, false)
end

function _alua.daemon.message.from_daemon(sock, context, header, reply)
	local reply_callback = function (__reply) reply(__reply) end
	message_common(sock, context, header, reply_callback, true)
end
