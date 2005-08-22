-- Copyright (c) 2005 Lab//, PUC-Rio
-- All rights reserved.

-- This file is part of ALua. As a consequence, to every excerpt of code
-- hereby obtained, the respective project's licence applies. Detailed
-- information regarding ALua's licence can be found in the LICENCE file.

-- Functions endorsing the exported API.
module("alua")

-- Encapsulation of external modules.
local aluad = require("aluad")
local event = require("event")
local netio = require("netio")
local utils = require("utils")
local chans = require("chans")
local timer = require("timer")

-- Export utils.dump() as alua.tostring().
tostring = utils.dump
-- Export the applications table.
applications = {}

-- Handler for incoming daemon messages.
local function
daemon_message(sock, context, body)
	-- Check for packet validity.
	if not body or not body.len or not body.from then
		-- Assumes infinite tolerance. May we stop sometime?
		return utils.bogus(sock, "daemon message", body)
	end

	-- Receive the message.
	local message, e = sock:receive(body.len)
	if not message then
		-- Probably a networking error, so we are better
		-- off discarding the current daemon.
		print("Error receiving message from daemon: " .. e)
		daemon_disconnect()
		return
	end

	-- Load the message into an executable object.
	local obj, e = loadstring(message)
	if not obj then
		print("Failed to load chunk received from " .. 
		    utils.dump(body.from) .. ": " .. e)
		return
	end

	-- And run it. Since exiting due to an incoming message's fault is not
	-- really what's wanted, do so in protected mode.
	local okay, e = pcall(obj)
	if not okay then
		print("Failed to execute chunk received from " ..
		    utils.dump(body.from) .. ": " .. e)
	end
end

-- Auxiliary function for issuing commands to the daemon.
local function
command(type, arg, callback)
	if not socket then
		-- If we are not connected yet, error out.
		callback({ status = "error", error = "Not connected" })
	else
		netio.cmd(socket, type, arg, callback)
	end
end

-- Auxiliary function for disconnecting from a daemon.
local function
daemon_disconnect()
	-- We are no longer in any application.
	applications = {}

	-- There's no socket to be used.
	if socket then
		event.del(socket)
		socket = nil
	end

	-- No daemon associated, and no identification.
	daemon = nil
	id = nil
end

-- Auxiliary function for connecting to a daemon.
function
daemon_connect(_socket, _daemon, _id)
	-- Okay, we are connected. Prepare the global environment.
	applications = {}
	socket = _socket
	daemon = _daemon
	id = _id

	-- Once we have a daemon, collect events from it.
	local cmds = { ["message"] = daemon_message }
	event.add(socket, { read = netio.handler }, { cmdtab = cmds })
end

-- Start processing events coming from the daemon as well as from channels.
function
loop()
	while true do
		-- If we run out of events, it's time to stop.
		if event.loop() == 0 then return end
		-- If there are any timers, check for them.
		if timercnt > 0 then timerpoll() end
	end
end

-- Terminate a process.
function
exit(processes, callback)
	-- If no processes were given, terminate the caller.
	if not processes then
		daemon_disconnect() -- In case we're still connected.
		os.exit()
	end

	-- Pass the termination call to the given processes.
	send(processes, "alua.exit()", callback)
end

-- Functions for managing applications. We need to provide a special callback
-- so that we can change the 'applications' table accordingly.

-- Leave an application.
function
leave(name, callback)
	local leave_callback = function(reply)
		if reply.status == "ok" then applications[name] = nil end
		if callback then callback(reply) end
	end

	command("leave", { name = name }, leave_callback)
end

-- Join an application.
function
join(name, callback)
	local join_callback = function(reply)
		if reply.status == "ok" then applications[name] = true end
		if callback then callback(reply) end
	end

	command("join", { name = name }, join_callback)
end

-- Start a new application.
function
start(name, callback)
	local start_callback = function(reply)
		if reply.status == "ok" then applications[name] = true end
		if callback then callback(reply) end
	end

	command("start", { name = name }, start_callback)
end

-- Link our daemon to other daemons.
function
link(daemons, authfs, callback)
	command("link", { daemons = daemons, authfs = authfs }, callback)
end

-- Send a message to a (group of) process(es).
function
send(to, msg, callback)
	-- Send the header, then the message.
	command("message", { to = to, len = string.len(msg) }, callback)
	socket:send(msg)
end

-- Spawn new processes in an application.
function
spawn(name, count, callback)
	command("spawn", { name = name, count = count }, callback)
end

-- Query the daemon about a given application.
function
query(name, callback)
	command("query", { name = name }, callback)
end

-- Create a new daemon.
function
create(conf)
	return aluad.create(conf)
end

-- Connect to a daemon.
function
connect(hash, authf)
	local _socket, id, e = aluad.connect(hash, "process", authf)
	if _socket then daemon_connect(_socket, hash, id) end
	return _socket
end

-- Open a connection with, or create a new a daemon.
function
open(arg)
	local _daemon, e = arg, nil

	-- If no argument was given, or it's a table...
	if not arg or type(arg) == "table" then
		-- Then create a new daemon.
		_daemon, e = aluad.create(arg)
		if not _daemon then return nil, e end
	end

	-- Now do a connection attempt to it.
	local _socket, _id, e = aluad.connect(_daemon, "process")
	if not _socket then return nil, e end

	daemon_connect(_socket, _daemon, _id)

	return _daemon
end
