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
-- This file implements the functions endorsing the exported API.

alua = {}

require("_alua.event")
require("_alua.netio")
require("_alua.utils")
require("_alua.timer")
require("_alua.daemon")
require("_alua.channel")

-- Handler for incoming daemon messages.
function alua.incoming_msg(sock, context, header, reply)
	local message, obj, okay, e
	-- Validate the header received.
	if not header or not header.len or not header.from then
		print("Invalid packet header received from daemon")
		alua.close()
		return
	end
	-- Receive the message.
	message, e = sock:receive(header.len)
	if not message then
		print("Error receiving message from daemon: " .. e)
		alua.close()
		return
	end
	-- Load the message into an executable object.
	obj, e = loadstring(message)
	if not obj then
		print("Failed to load chunk received from " .. 
		    _alua.utils.dump(header.from) .. ": " .. e)
		return
	end

	reply({ to = alua.id, status = "ok" })

	-- And run it.
	okay, e = pcall(obj)
	if not okay then
		print("Failed to execute chunk received from " ..
		    _alua.utils.dump(header.from) .. ": " .. e)
	end
end

-- Auxiliary function for issuing commands to the daemon.
function alua.command(type, arg, callback)
	if not alua.socket then
		-- If we are not connected yet, error out.
		if callback then
			callback({ status = "error", error = "Not connected" })
		end
	else
		_alua.netio.async(alua.socket, type, arg, callback)
	end
end

-- The main event loop of an ALua process.
function alua.loop()
	while true do
		-- If we run out of events, it's time to stop.
		if _alua.event.loop() == 0 then
			return
		end
		-- If there are any timers, check for them.
		if _alua.timer.active_count > 0 then
			_alua.timer.poll()
		end
	end
end

-- Terminate a process.
function
alua.exit(processes, callback, code)
	-- If no processes were given, terminate the caller.
	if not processes then
		alua.close()
		os.exit(code)
	end
	-- Pass the termination call to the given processes.
	alua.send(processes, "alua.exit()", callback)
end

-- Functions for managing applications. We need to provide a special callback
-- so that we can change the 'applications' table accordingly.

-- Leave an application.
function
alua.leave(name, callback)
	local leave_callback = function(reply)
		if reply.status == "ok" then
			alua.applications[name] = nil
		end
		if callback then
			callback(reply)
		end
	end
	alua.command("leave", { name = name }, leave_callback)
end

-- Join an application.
function
alua.join(name, callback)
	local join_callback = function(reply)
		if reply.status == "ok" then
			alua.applications[name] = true
		end
		if callback then
			callback(reply)
		end
	end
	alua.command("join", { name = name }, join_callback)
end

-- Start a new application.
function
alua.start(name, callback)
	local start_callback = function(reply)
		if reply.status == "ok" then
			alua.applications[name] = true
		end
		if callback then
			callback(reply)
		end
	end
	alua.command("start", { name = name }, start_callback)
end

-- Link our daemon to other daemons.
function
alua.link(daemons, authfs, callback)
	alua.command("link", { daemons = daemons, authfs = authfs }, callback)
end

-- Send a message to a (group of) process(es).
function
alua.send(to, msg, callback, timeout)
	-- Send the header, then the message.
	alua.command("message", { to = to, len = string.len(msg),
	    timeout = timeout }, callback)
	alua.socket:send(msg)
end

-- Spawn new processes in an application.
function
alua.spawn(name, count, callback)
	alua.command("spawn", { name = name, count = count }, callback)
end

-- Query the daemon about a given application.
function
alua.query(name, callback)
	alua.command("query", { name = name }, callback)
end

-- Connect to a daemon.
function alua.connect(daemon, auth_callback)
	local socket, commands, callback, id, e
	-- If we're already connected, error out.
	if alua.socket then return nil, "Already connected" end
	-- Otherwise, try connecting.
	socket, id, e = _alua.daemon.connect(daemon, "process", auth_callback)
	if not socket then return nil, e end
	-- Okay, we have a daemon. Prepare the environment.
	alua.socket = socket
	alua.daemon = daemon
	alua.id = id
	-- Collect events from the daemon.
	commands = { ["message"] = alua.incoming_msg }
	callback = { read = _alua.netio.handler }
	_alua.utils.protect(commands, _alua.utils.invalid_command)
	_alua.event.add(socket, callback, { command_table = commands })
	return daemon
end

-- Open a connection with, or create a new a daemon.
function alua.open(arg)
	local daemon, e
	-- If there's no argument, or it's a table, then create a new daemon.
	if not arg or type(arg) == "table" then
		daemon, e = _alua.daemon.create(arg)
		if not daemon then return nil, e end
	end
	-- Now do a connection attempt to it.
	return alua.connect(daemon or arg)
end

-- Close the connection with the current daemon.
function alua.close(arg)
	if arg then return _alua.channel.close(arg) end
	-- If we're not connected, error out.
	if not alua.socket then return nil, "Not connected" end
	-- Leave every application we are in.
	for _, app in alua.applications do alua.leave(app) end
	_alua.event.del(alua.socket)
	alua.applications = {} -- Reset alua.applications now
	alua.socket = nil
	alua.daemon = nil
	alua.id = nil
end

-- Prepare the 'alua' table.
alua.create = _alua.daemon.create
alua.tostring = _alua.utils.dump
alua.applications = {}
-- Provide simple shells for the timer functions.
alua.timeradd = _alua.timer.add
alua.timerdel = _alua.timer.del
-- Provide simple shells for the channel functions.
alua.setpattern = _alua.channel.setpattern
alua.getpattern = _alua.channel.getpattern
alua.client = _alua.channel.client
alua.server = _alua.channel.server

return alua
