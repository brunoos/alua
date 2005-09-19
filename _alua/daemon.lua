-- $Id$

-- Copyright (c) 2005 Lab//, PUC-Rio
-- All rights reserved.

-- This file is part of ALua. As a consequence, to every excerpt of code
-- hereby obtained, the respective project's licence applies. Detailed
-- information regarding ALua's licence can be found in the LICENCE file.

-- Main body for the ALua daemon.
module("_alua.daemon")

-- Encapsulation of external modules.
require("socket")
require("posix")
require("_alua.event")
require("_alua.netio")
require("_alua.utils")
require("_alua.message")

local daemons = {}
_alua.daemon.apptable = {}
local idcount = 0 -- Count of local process IDs.
local current_daemon_idx = 0

-- Auxiliary function for generating new IDs.
local function
newid()
	local id = string.format("%s:%u", conf.hash, idcount)
	idcount = idcount + 1
	return id
end

-- Check if a given application exists.
local function
process_query(sock, context, arg, reply)
	-- Look up the application.
	local app = _alua.daemon.apptable[arg.name]
	if not app then
		-- Application does not exist.
		reply({ name = arg.name })
		return
	end
	-- To avoid constructing the same tables again and again, keep a cache
	-- which is invalidated everytime a new process comes in or leaves.
	if not app.cache then
		-- Construct the tables, and cache them.
		local ptab, dtab = {}, {}
		for i in pairs(app.ptab) do table.insert(ptab, i) end
		app.cache = { ptab = ptab }
		app.cache.name = arg.name
		app.cache.master = app.master
	end
	reply({ master = app.cache.master, name = app.cache.name,
	    processes = app.cache.ptab })
end

-- A remote daemon is telling us about a new application.
local function
daemon_start(sock, context, arg, reply)
	_alua.daemon.apptable[arg.name] = arg
	_alua.daemon.apptable[arg.name].ptab = {}
	_alua.daemon.apptable[arg.name].ptab[arg.master] = sock
end

-- Start a new application.
local function
process_start(sock, context, arg, reply)
	-- Make sure the application does not exist.
	local app = _alua.daemon.apptable[arg.name]
	if app then
		reply({ name = arg.name, status = "error",
		        error = "Application already exists" })
		return
	end
	-- Initialize the object that is going to represent it.
	-- Insert the master in the process table.
	app = { master = context.id, ptab = {}, name = arg.name }
	app.ptab[app.master] = sock
	-- Prepare the reply.
	local _reply = { name = arg.name, status = "ok" }
	-- Save the application in the global table.
	-- And associate the master process with it.
	_alua.daemon.apptable[arg.name] = app
	context.apptable[arg.name] = app
	-- Tell our fellow daemons about this new application.
	for i, s in daemons do
		_alua.netio.async(s, "start", { name = arg.name,
		    master = context.id })
	end
	reply(_reply)
end

-- Auxiliar function for spawning a new process.
function
spawn(context, app, id)
	-- Since there's no socketpair(), create two sockets
	-- and connect them manually to each other.
	local s1 = socket.bind("127.0.0.1", 0)
	local s2 = socket.connect(s1:getsockname())

	s1 = s1:accept()

	-- Actually create a new process.
	local f = posix.fork()

	-- Check for fork() failure.
	if f < 0 then
		s1:close()
		s2:close()
		return "error", "Fork failed"
	end

	-- If we are the daemon, create a state for it and return. We must be
	-- careful here, and simulate a real connecting process context.
	if f == 0 then
		s2:close()
		local new_context = { apptable = { [app.name] = app }, id = id,
				      command_table = process_command_table }
		_alua.event.add(s1, { read = _alua.netio.handler }, new_context)

		-- Associate the process with the application.
		-- And invalidate the application process cache.
		app.ptab[id] = s1
		app.cache = nil

		return "ok", nil, id, conf.hash
	end

	s1:close()
	_alua.event.flush() -- Get rid of past events states.

	-- Okay, we are the new process. Prepare the 'alua'
	-- environment, and fall into the event loop.
	s1:close()
	local alua = require("alua")
	alua.applications = {}
	alua.applications[app.name] = true
	alua.master = app.master
	alua.parent = context.id
	alua.socket = s2
	alua.id = id
	commands = { ["message"] = alua.incoming_msg }
	callback = { read = _alua.netio.handler }
	_alua.event.add(s2, callback, { command_table = commands })
	alua.loop()
	os.exit()
end

local function
spawn_local(context, app, name, callback)
        -- If a name was provided, make sure it doesn't exist already.
        if name then
                if app.ptab[name] then
                        return "error", "Name already in use"
                end
        else
                -- Otherwise, get a new name for the process.
                name = newid()
        end

        local status, e, id, daemon = spawn(context, app, name)
	local ret = { status = status, error = e, id = id, daemon = daemon }

	if ret.status == "ok" then
		-- Warn the other daemons about this new process.
		for _, sock in daemons do
			_alua.netio.async(sock, "notify",
			    { app = app.name, id = ret.id })
		end
	end

	if callback then callback(ret) else return ret end
end

local function
spawn_forward(hash, app, name, callback)
	local sock = daemons[hash]
	fake = true
	_alua.netio.async(sock, "spawn", { app = app.name, name = name }, callback)
end

-- Spawn switch. Takes care of distributing the processes
-- between the possibly many existing daemons.
local function
spawn_switch(context, app, name, callback)
	if not current_spawn_daemon then
		spawn_local(context, app, name, callback)
	else
		-- Get the current daemon, and forward the request to it.
		spawn_forward(current_spawn_daemon, app, name, callback)
	end

	-- Update 'current_spawn_daemon's value.
	current_spawn_daemon = next(daemons, current_spawn_daemon)
end

-- Spawn new processes. Operates asynchronously.
local function
process_spawn(sock, context, arg, reply)
	local count
	-- Make sure the application exists.
	local app = _alua.daemon.apptable[arg.name]
	if not app then
		reply({ name = arg.name, status = "error",
		        error = "Application does not exist" })
		return
	end
	-- And that the requesting process is in it.
	if not context.apptable[arg.name] then
		reply({ name = arg.name, status = "error",
		        error = "Not in such application" })
		return
	end
	if type(arg.count) == "table" then
		count = table.getn(arg.count)
	else
		count = arg.count
	end
	local ptab, done = {}, 0
	local callback = function(_reply)
		ptab[_reply.id] = { status = _reply.status,
				   error = _reply.error,
				   daemon = _reply.daemon }
		done = done + 1
		if done == count then
			-- Time to send the reply.
			reply({ name = arg.name, processes = ptab })
		end
	end
	-- Launch requests for every process, and return.
	if type(arg.count) == "table" then
		-- If the argument received is a table, then it must hold the
		-- name of the processes we are about to create. Use it.
		for _, name in arg.count do
			spawn_switch(context, app, name, callback)
		end
	else
		-- Otherwise, create the requested amount of processes.
		while arg.count > 0 do
			spawn_switch(context, app, nil, callback)
			arg.count = arg.count - 1
		end
	end
end

-- Forwarded request for new processes.
local function
daemon_spawn(sock, context, arg, reply)
	local app = _alua.daemon.apptable[arg.app]
	reply(spawn_local(app, app, arg.name))
end

-- Extend our network of daemons.
local function
process_link(sock, context, arg, reply)
	-- Just iterate over the given array of daemons,
	-- opening connections to each one of them.
	local _reply = { daemons = {}, status = "ok" }
	for _, hash in arg.daemons or {} do
		if arg.authfs then
			local f = loadstring(arg.authfs)
		end
		if hash == 0 then break end
		local sock, id, e = connect(hash, "daemon", f)
		if not sock then
			_reply.daemons[hash] = e -- Error
		else
			_reply.daemons[hash] = "Ok"
			-- Forward the link request, so the remote daemons can
			-- also create connections between themselves.
			_alua.netio.async(daemons[hash], "link", arg.daemons)
		end
	end

	reply(_reply)
end

-- Extend our network of daemons, request coming from a daemon.
local function
daemon_link(sock, context, arg, reply)
	process_link(sock, context, arg, reply)
end

-- A daemon is telling us about a new process belonging to it.
local function
daemon_notify(sock, context, arg, reply)
	local app = _alua.daemon.apptable[arg.app]
	app.ptab[arg.id] = sock
	app.cache = nil
end

-- Associate a process with an application.
local function
process_join(sock, context, arg, reply)
	-- Make sure the application exists.
	local app = _alua.daemon.apptable[arg.name]
	if not app then
		reply({ name = arg.name, status = "error",
		        error = "Application does not exist" })
		return
	end
	-- And that the requesting process is not in it.
	if context.apptable[arg.name] then
		reply({ name = arg.name, status = "error",
			error = "Already in application" })
		return
	end
	-- Associate the process with the application.
	context.apptable[arg.name] = app
	app.ptab[context.id] = sock
	-- Invalidate the application's process cache.
	app.cache = nil
	-- We must return information about the joined
	-- application, so simulate a query.
	process_query(sock, context, { name = arg.name }, reply)
end

-- Leave an application.
local function
process_leave(sock, context, arg, reply)
	-- Make sure the application exists.
	local app = _alua.daemon.apptable[arg.name]
	if not app then
		reply({ name = arg.name, status = "error",
			error = "Application does not exist" })
		return
	end
	-- And that the requesting process is in it.
	if not context.apptable[arg.name] then
		reply({ name = arg.name, status = "error",
			error = "Not in such application" })
		return
	end
	-- Deassociate the process from the application.
	context.apptable[arg.name] = nil
	app.ptab[context.id] = nil
	-- Invalidate the application's process cache.
	app.cache = nil
	reply({ name = arg.name, status = "ok" })
end

-- Authenticate a remote endpoint, either as a process or a daemon.
function
proto_auth(sock, context, arg, reply)
	context.mode = arg.mode
	context.apptable = {}

	if arg.mode == "process" then
		-- Get an ID for the new process.
		context.id = newid()
		-- Set the connection to a 'process' context.
		context.command_table = process_command_table
	end

	if arg.mode == "daemon" then
		context.id = arg.id
		-- Set the connection to a 'daemon' context.
		context.command_table = _alua.daemon.command_table
		-- And mark that we now have a connection with that daemon.
		daemons[context.id] = sock
	end

	reply({ id = context.id })
end

-- Connect to a daemon.
function
connect(hash, mode, authf)
	if not conf then conf = default_conf end
	if daemons[hash] then return nil, nil, "Already connected" end
	local sock, e = socket.connect(_alua.utils.unhash(hash))
	if not sock then return nil, nil, e end
	-- Authenticate synchronously, the channel is not shared at this point.
	local reply, _, e = _alua.netio.sync(sock, "auth", { mode = mode,
	    id = conf.hash })
	-- Run an authentication function, if it was provided.
	if authf then authf(hash, mode, sock) end
	daemons[hash] = sock
	if mode == "daemon" then
		_alua.event.add(sock, { read = _alua.netio.handler },
		    { command_table = _alua.daemon.command_table })
	end
	return sock, reply.arguments.id
end

-- Dequeue an incoming connection, set it to a raw context.
function
aluad_connection(sock, context)
	local ic, e = sock:accept()
	if not ic then
		print("Failed to dequeue incoming connection: " .. e)
	else
		-- A connection in a raw context can only do 'auth'.
		local commands = { ["auth"] = proto_auth }
		_alua.event.add(ic, { read = _alua.netio.handler },
		    { command_table = commands })
	end
end

-- Default configuration table.
default_conf = {
	addr = "*",	-- Address to listen for incoming connections.
	port = 6080,	-- Port to listen for incoming connections.
}

-- Create a new daemon, as requested by the user.
function
create(uconf)
	conf = default_conf
	-- If a configuration table was given, load it.
	if uconf then for i, v in pairs(uconf) do conf[i] = v end end

	-- Bind our listening socket and get a copy of our hash.
	local sock, e = socket.bind(conf.addr, conf.port)
	if not sock then return nil, e end
	conf.hash = _alua.utils.hash(sock:getsockname())

	-- Fork, and dispatch from the calling process.
	local f, e = posix.fork()
	if not f then return nil, e end
	if f > 0 then return conf.hash end

	-- Add an event for incoming connections.
	_alua.event.add(sock, { read = aluad_connection })

	-- Then loop, as every happy daemon should.
	while true do _alua.event.loop() end
end

process_command_table = {
	["link"] = process_link,
	["join"] = process_join,
	["start"] = process_start,
	["query"] = process_query,
	["spawn"] = process_spawn,
	["message"] = _alua.daemon.message.from_process,
	["leave"] = process_leave,
}

_alua.daemon.command_table = {
	["link"] = daemon_link,
	["start"] = daemon_start,
	["spawn"] = daemon_spawn,
	["notify"] = daemon_notify,
	["message"] = _alua.daemon.message.from_daemon,
}

-- Protect access to the tables above
_alua.utils.protect(process_command_table, _alua.utils.invalid_command)
_alua.utils.protect(_alua.daemon.command_table, _alua.utils.invalid_command)
