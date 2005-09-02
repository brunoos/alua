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

module("alua")

require("_alua.event")
require("_alua.netio")
require("_alua.utils")
require("_alua.timer")
require("_alua.daemon")
require("_alua.channel")

-- Handler for incoming daemon messages.
function alua.incoming_msg(sock, context, header)
	local message, obj, okay, e
	-- Validate the header received.
	if not header or not header.len or not header.from then
		print("Invalid packet header received from daemon")
		-- alua.close()
		return
	end
	-- Receive the message.
	message, e = sock:receive(header.len)
	if not message then
		print("Error receiving message from daemon: " .. e)
		-- alua.close()
		return
	end
	-- Load the message into an executable object.
	obj, e = loadstring(message)
	if not obj then
		print("Failed to load chunk received from " .. 
		    _alua.utils.dump(body.from) .. ": " .. e)
		return
	end
	-- And run it.
	okay, e = pcall(obj)
	if not okay then
		print("Failed to execute chunk received from " ..
		    _alua.utils.dump(body.from) .. ": " .. e)
	end
end

-- Auxiliary function for issuing commands to the daemon.
local function
command(type, arg, callback)
	if not socket then
		-- If we are not connected yet, error out.
		callback({ status = "error", error = "Not connected" })
	else
		_alua.netio.cmd(socket, type, arg, callback)
	end
end

-- Auxiliary function for disconnecting from a daemon.
local function
daemon_disconnect()
	-- We are no longer in any application.
	alua.applications = {}

	-- There's no socket to be used.
	if socket then
		_alua.event.del(socket)
		alua.socket = nil
	end

	-- No daemon associated, and no identification.
	alua.daemon = nil
	alua.id = nil
end

-- Auxiliary function for connecting to a daemon.
function
daemon_connect(_socket, _daemon, _id)
	-- Okay, we are connected. Prepare the global environment.
	alua.applications = {}
	alua.socket = _socket
	alua.daemon = _daemon
	alua.id = _id

	-- Once we have a daemon, collect events from it.
	local cmds = { ["message"] = alua.incoming_msg }
	_alua.event.add(socket, { read = _alua.netio.handler },
	    { cmdtab = cmds })
end

-- Start processing events coming from the daemon as well as from channels.
function
alua.loop()
	while true do
		-- If we run out of events, it's time to stop.
		if _alua.event.loop() == 0 then return end
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
		daemon_disconnect() -- In case we're still connected.
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
		if reply.status == "ok" then alua.applications[name] = nil end
		if callback then callback(reply) end
	end

	command("leave", { name = name }, leave_callback)
end

-- Join an application.
function
alua.join(name, callback)
	local join_callback = function(reply)
		if reply.status == "ok" then alua.applications[name] = true end
		if callback then callback(reply) end
	end

	command("join", { name = name }, join_callback)
end

-- Start a new application.
function
alua.start(name, callback)
	local start_callback = function(reply)
		if reply.status == "ok" then alua.applications[name] = true end
		if callback then callback(reply) end
	end

	command("start", { name = name }, start_callback)
end

-- Link our daemon to other daemons.
function
alua.link(daemons, authfs, callback)
	command("link", { daemons = daemons, authfs = authfs }, callback)
end

-- Send a message to a (group of) process(es).
function
alua.send(to, msg, callback)
	-- Send the header, then the message.
	command("message", { to = to, len = string.len(msg) }, callback)
	socket:send(msg)
end

-- Spawn new processes in an application.
function
alua.spawn(name, count, callback)
	command("spawn", { name = name, count = count }, callback)
end

-- Query the daemon about a given application.
function
alua.query(name, callback)
	command("query", { name = name }, callback)
end

-- Create a new daemon.
function
alua.create(conf)
	return _alua.daemon.create(conf)
end

-- Connect to a daemon.
function
alua.connect(hash, authf)
	local _socket, id, e = _alua.daemon.connect(hash, "process", authf)
	if _socket then daemon_connect(_socket, hash, id) end
	return _socket
end

-- Open a connection with, or create a new a daemon.
function
alua.open(arg)
	local _daemon, e = arg, nil

	-- If no argument was given, or it's a table...
	if not arg or type(arg) == "table" then
		-- Then create a new daemon.
		_daemon, e = _alua.daemon.create(arg)
		if not _daemon then return nil, e end
	end

	-- Now do a connection attempt to it.
	local _socket, _id, e = _alua.daemon.connect(_daemon, "process")
	if not _socket then return nil, e end

	daemon_connect(_socket, _daemon, _id)

	return _daemon
end

-- Close the connection with the current daemon.
function alua.close()
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
alua.close = _alua.channel.close
