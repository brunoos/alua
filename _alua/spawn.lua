-- $Id$
-- copyright (c) 2005 pedro martelletto <pedro@ambientworks.net>
-- all rights reserved. part of the alua project.

module("_alua.daemon.spawn")

require("socket")
require("posix")
require("_alua.app")
require("_alua.event")
require("_alua.netio")

-- loop for the spawned process
local function spawn_loop(context, app, id, s2)
	_alua.event.flush() -- get rid of past events states
	local alua = require("alua")
	alua.applications = {}; alua.applications[app.name] = true
	alua.master = app.master; alua.parent = context.id
	alua.socket = s2; alua.id = id
	local commands = { ["message"] = alua.incoming_msg }
	local callback = { read = _alua.netio.handler }
	_alua.event.add(s2, callback, { command_table = commands })
	alua.loop(); os.exit()
end

-- spawns a local process
local function spawn(context, app, id)
	local s1 = socket.bind("127.0.0.1", 0)
	local s2 = socket.connect(s1:getsockname())
	s1 = s1:accept() -- simulate what socketpair() would do
	local f = posix.fork()
	if not f then -- fork() failed
		s1:close(); s2:close(); return id, "error", "fork failed" end
	if f > 0 then s1:close(); spawn_loop(context, app, id, s2) end
	local _context = { apptable = { [app.name] = app }, id = id,
			   command_table = _alua.daemon.process_command_table }
	_alua.event.add(s1, { read = _alua.netio.handler }, _context)
	app.processes[id] = s1; app.cache = nil -- invalidate cache
	s2:close(); return id, "ok"
end

local function spawn_local(context, master, app, name)
	if not name then name = _alua.daemon.get_new_process_id() end
	if app.processes[name] then
		return name, "error", "name already in use" end
        local id, status, e = spawn(context, app, name)
	if status == "ok" then -- notify daemons of new process
		for v, sock in app.daemons do -- don't notify ourselves
			if v ~= _alua.daemon.self.hash  then
				_alua.netio.async(sock, "notify",
				{ app = app.name, id = id }) end
		end
	end; return id, status, e
end

local function spawn_distribute(context, app, reply, spawn_table, entries)
	local reply_table = { name = app.name, processes = {} }
	local callback = function (spawn_reply)
		for id, status in spawn_reply.processes do
		reply_table.processes[id] = status end; entries = entries - 1
		if entries == 0 then reply(reply_table) end -- time to reply
	end
	for daemon, args in spawn_table do -- send all the spawn requests
		local sock = app.daemons[daemon]
		args.app = app.name; args.parent = context.id
		_alua.netio.async(sock, "spawn", args, callback) end
end

local function spawn_get_names(names, count)
	if not names then return {}, {} end
	local remaining, used, n = {}, {}, table.getn(names)
	for i = n, count + 1, - 1 do table.insert(remaining, names[i]) end
	for i = count, 1, - 1 do table.insert(used, names[i]) end
	return remaining, used
end

local function spawn_prepare_table(context, argument, reply, app)
	local spawn_table, count = {}, argument.processes
	local perdaemon = math.floor(count / app.ndaemons)
	local mod, entries = math.mod(count, app.ndaemons), 0
	local remaining = argument.names
	for daemon in app.daemons do -- fill in the spawn table
		local entry = { count = perdaemon }
		if mod > 0 then entry.count = entry.count + 1; mod = mod - 1 end
		remaining, entry.names = spawn_get_names(remaining, entry.count)
		spawn_table[daemon] = entry; entries = entries + 1
	end; spawn_distribute(context, app, reply, spawn_table, entries)
end

local function spawn_by_table(context, argument, reply, app)
	local spawn_table, daemons, entries = {}, argument.processes, 0
	for daemon, processes in daemons do
		entries = entries + 1
		if type(processes) == "number" then
			spawn_table[daemon] = { count = processes, names = {} }
		else	spawn_table[daemon] = { count = table.getn(processes),
						names = processes } end
	end; spawn_distribute(context, app, reply, spawn_table, entries)
end

local function spawn_by_name(context, argument, reply, app)
	argument.names = argument.processes
	argument.processes = table.getn(argument.processes)
	spawn_prepare_table(context, argument, reply, app)
end

function _alua.daemon.spawn.from_process(sock, context, argument, reply)
	local app = _alua.daemon.app.verify_proc(context, argument.name, reply)
	if not app then return end -- process not in application
	if type(argument.processes) == "number" then
		spawn_prepare_table(context, argument, reply, app)
	elseif type(argument.processes) == "table" then
		if type(argument.processes[1]) == "string" then
			spawn_by_name(context, argument, reply, app)
		else spawn_by_table(context, argument, reply, app)  end
	end
end

function _alua.daemon.spawn.from_daemon(sock, context, argument, reply)
	local app, processes = _alua.daemon.app.apptable[argument.app], {}
	for i = argument.count, 1, - 1 do
		local id, status, e = spawn_local(context, argument.master,
						  app, argument.names[i])
		processes[id] = { daemon = _alua.daemon.self.hash,
				  status = status, error = e }
	end; reply({ processes = processes })
end
