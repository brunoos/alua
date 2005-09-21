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
local function msg_deliver(context, header, msg, callback)
	if context.command_table == _alua.daemon.command_table then
		context.apptable = _alua.daemon.apptable end
	for _, app in context.apptable do
		local to = header.to; local s = app.processes[to]
		if s then local timer = _alua.timer.add(function(t)
			callback({ to = to, status = "error",
				   error = "timeout" })
			_alua.timer.del(t) end, header.timeout)
			_alua.netio.async(s, "message", header, function(reply)
				callback(reply); _alua.timer.del(timer) end)
			s:send(msg) end; end
end

-- Receive a message from a process and deliver it.
local function message_common(sock, context, header, reply, forwarding)
	local msg, e = sock:receive(header.len)
	if not header.from then header.from = context.id end
	if type(header.to) == "table" and not forwarding then
		for _, dest in header.to do -- fake new header
			local newheader = header; newheader.to = dest
			msg_deliver(context, newheader, msg, reply) end
	else msg_deliver(context, header, msg, reply) end
end

-- Receive a message from a process and deliver it.
function _alua.daemon.message.from_process(sock, context, header, reply)
	local done, _reply, to = {}, {}, header.to
	local count = type(to) == "table" and table.getn(to) or 1
	local reply_callback = function (msg)
		_reply[msg.to] = { status = msg.status, error = msg.error }
		count = count - 1; if count == 0 then reply(_reply) end
	end; message_common(sock, context, header, reply_callback, false)
end

function _alua.daemon.message.from_daemon(sock, context, header, reply)
	local reply_callback = function (__reply) reply(__reply) end
	message_common(sock, context, header, reply_callback, true)
end
