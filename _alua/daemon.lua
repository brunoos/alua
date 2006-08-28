-- $Id$

-- Copyright (c) 2005 Pedro Martelletto <pedro@ambientworks.net>
-- All rights reserved. Part of the ALua project.

module("_alua.daemon")

-- external modules
require("socket")
require("posix")

-- internal modules
require("_alua.event")
require("_alua.netio")
require("_alua.utils")
require("_alua.spawn")
require("_alua.message")

daemontable = {}

local idcount = 0 -- count of local processes

-- generate a new process id
function _alua.daemon.get_new_process_id()
	local id = string.format("%s:%u", _alua.daemon.self.hash, idcount)

	idcount = idcount + 1
	
	return id
end

-- get a connection with a daemon
function _alua.daemon.get(hash, callback)
	local s = daemontable[hash]
	
	if s then -- already connected
		if callback then
			callback(s)
		end
		
		return s
	end

	local s, e = socket.connect(_alua.daemon.unhash(hash))
	local _context = { command_table = _alua.daemon.command_table }
	local _callback = { read = _alua.netio.handler }

	_alua.event.add(s, _callback, _context)
	
	if callback then -- async
		local f = function (reply)
			callback(s)
		end

		_alua.netio.async(s, "auth", { mode = "daemon",
		    id = _alua.daemon.self.hash }, f)
	else
		if not s then
			return nil, e
		end

		_alua.netio.sync(s, "auth", { mode = "daemon",
		    id = _alua.daemon.self.hash })

	end
	
	daemontable[hash] = s
	
	return s
end

-- hash an (address, port, id) set
function _alua.daemon.hash(addr, port)
        if addr == "0.0.0.0" then -- workaround
		addr = "127.0.0.1"
	end

        return string.format("%s:%u", addr, port)
end

-- unhash a (address, port, id) set
function _alua.daemon.unhash(hash)
        local _, _, addr, port, id = string.find(hash, "(%d.+):(%d+)")

        return addr, tonumber(port), id
end

-- Extend our network of daemons.
local function process_link(sock, context, argument, reply, noforward)
	local app = _alua.daemon.app.verify_proc(context, argument.name, reply)

	if not app then -- process not in application
		return nil
	end

	local _reply = { daemons = {}, status = "ok" }

	for _, hash in pairs(argument.daemons or {}) do
		local sock, id, e = _alua.daemon.get(hash)

		if not sock then
			_reply.daemons[hash] = e
		else
			_reply.daemons[hash] = "ok"
			app.daemons[hash] = sock
			app.ndaemons = app.ndaemons + 1
			argument.master = app.master

			-- Forward link request
			if _alua.daemon.self.hash ~= hash and not noforward then
				_alua.netio.async(sock, "link", argument)
			end
		end
	end

	reply(_reply)
end

-- Extend our network of daemons, request coming from a daemon.
local function daemon_link(sock, context, argument, reply)
	local app = { master = argument.master, processes = {},
	    name = argument.name }

	app.processes[app.master] = sock
	_alua.daemon.app.apptable[argument.name] = app
	context.apptable[argument.name] = app

	local callback = function (s)
		app.ndaemons = 1
		app.daemons = {}
		app.daemons[_alua.daemon.self.hash] = s
		app.daemons[context.id] = sock
		process_link(sock, context, argument, reply, true)
	end

	_alua.daemon.get(_alua.daemon.self.hash, callback)
end

-- Authenticate a remote endpoint, either as a process or a daemon.
local function proto_auth(sock, context, argument, reply)
	context.mode = argument.mode
	context.apptable = {}

	if argument.mode == "process" then
		context.id = _alua.daemon.get_new_process_id()
		context.command_table = _alua.daemon.process_command_table
	end

	if argument.mode == "daemon" then
		context.id = argument.id
		context.command_table = _alua.daemon.command_table
	end

	reply({ id = context.id })
end

-- Dequeue an incoming connection, set it to a raw context.
function _alua.daemon.incoming_connection(sock, context)
        local incoming_sock, e = sock:accept()
	local commands = { ["auth"] = proto_auth }
        local callback = { read = _alua.netio.handler }

        _alua.event.add(incoming_sock, callback, { command_table = commands })
end

-- Create a new daemon, as requested by the user.
function _alua.daemon.create(user_conf)
        local sock, callback, f, e

        _alua.daemon.self = { addr = "*", port = 6080 }

        if user_conf then
		for i, v in pairs(user_conf) do
			_alua.daemon.self[i] = v
		end
	end

        sock, e = socket.bind(_alua.daemon.self.addr, _alua.daemon.self.port)

        if not sock then
		return nil, e
	end

        _alua.daemon.self.socket = sock
        _alua.daemon.self.hash = _alua.daemon.hash(sock:getsockname())
        f, e = posix.fork()

        if not f then -- fork() failed
		return nil, e
	end

        if f > 0 then -- parent
		return _alua.daemon.self.hash
	end

        callback = { read = _alua.daemon.incoming_connection }
        _alua.event.add(_alua.daemon.self.socket, callback)

        while true do
		_alua.event.loop()
		_alua.timer.poll()
	end
end

-- Connect to a daemon, as requested by the user.
function _alua.daemon.connect_process(daemon, auth_callback)
        local sock, e = socket.connect(_alua.daemon.unhash(daemon))

        if not sock then
		return nil, nil, e
	end

        local reply, e = _alua.netio.sync(sock, "auth", { mode = "process" })

        if not reply then
		return nil, nil, e
	end

        return sock, reply.arguments.id
end

_alua.daemon.process_command_table = {
	["link"] = process_link,
	["join"] = _alua.daemon.app.join,
	["start"] = _alua.daemon.app.start,
	["query"] = _alua.daemon.app.query,
	["spawn"] = _alua.daemon.spawn.from_process,
	["message"] = _alua.daemon.message.from_process,
	["leave"] = _alua.daemon.app.leave,
}

_alua.daemon.command_table = {
	["link"] = daemon_link,
	["spawn"] = _alua.daemon.spawn.from_daemon,
	["notify"] = _alua.daemon.app.notify,
	["message"] = _alua.daemon.message.from_daemon,
}

_alua.utils.protect(_alua.daemon.process_command_table,
    _alua.utils.invalid_command)
_alua.utils.protect(_alua.daemon.command_table,
    _alua.utils.invalid_command)
