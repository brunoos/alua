-- $Id$
-- copyright (c) 2005 pedro martelletto <pedro@ambientworks.net>
-- all rights reserved. part of the alua project.

alua = {}

require("_alua.event")
require("_alua.netio")
require("_alua.utils")
require("_alua.timer")
require("_alua.daemon")
require("_alua.channel")

-- handler for incoming daemon messages
function alua.incoming_msg(sock, context, header, reply)
	local message, e = sock:receive(header.len) -- receive the message
	if not message then alua.close(); return end
	local obj = loadstring(message) -- load message into executable object
	local _exit = os.exit; os.exit = function(code)
		reply({ to = alua.id, status = "ok" }); _exit(code)
	end -- if we exit(), reply first
	local okay, e = pcall(obj); os.exit = _exit
	reply({ to = alua.id, status = okay and "ok" or "error", error = e })
end

-- issue commands to the daemon
function alua.command(type, arg, callback)
	if not alua.socket then if callback then -- error out
		callback({ status = "error", error = "not connected" }) end
	else _alua.netio.async(alua.socket, type, arg, callback) end
end

-- main event loop
function alua.loop()
	while true do _alua.event.loop(); _alua.timer.poll(); end
end

-- terminate a (set of) process(es)
function alua.exit(to, code, callback)
	if not to then os.exit(code) end; code = code or "nil"
	alua.send(to, "alua.exit(nil, nil, " .. code .. ")", callback)
end

-- link daemons to daemons
function alua.link(app, daemons, callback)
	alua.command("link", { name = app, daemons = daemons }, callback)
end

-- send a message to a (set of) process(es)
function alua.send(to, msg, callback, timeout)
	alua.command("message", { to = to, len = string.len(msg),
				  timeout = timeout }, callback)
	alua.socket:send(msg)
end

-- spawn new processes in an application
function alua.spawn(name, processes, callback)
	alua.command("spawn", { name = name, processes = processes }, callback)
end

-- connect to a daemon. operates synchronously
function alua.connect(daemon)
	if alua.socket then return nil, "already connected" end
	local socket, id, e = _alua.daemon.connect_process(daemon)
	if not socket then return nil, e end
	alua.socket = socket; alua.daemon = daemon; alua.id = id
	local commands = { ["message"] = alua.incoming_msg }
	local callback = { read = _alua.netio.handler }
	_alua.utils.protect(commands, _alua.utils.invalid_command)
	_alua.event.add(socket, callback, { command_table = commands })
	return daemon
end

-- open a connection with, or create a new a daemon
function alua.open(arg)
	local daemon, e
	if not arg or type(arg) == "table" then
		daemon, e = _alua.daemon.create(arg)
		if not daemon then return nil, e end
	end; return alua.connect(daemon or arg)
end

-- close the connection with the current daemon
function alua.close(arg)
	if arg then return _alua.channel.close(arg) end
	if not alua.socket then return nil, "not connected" end
	for _, app in pairs(alua.applications) do alua.leave(app) end
	_alua.event.del(alua.socket)
	alua.applications = {} -- reset alua.applications now
	alua.socket = nil; alua.daemon = nil; alua.id = nil
end

-- prepare the 'alua' table
alua.create = _alua.daemon.create
alua.tostring = _alua.utils.dump
alua.applications = {}
-- provide simple shells for the timer functions
alua.timeradd = _alua.timer.add
alua.timerdel = _alua.timer.del
-- provide simple shells for the channel functions
alua.setpattern = _alua.channel.setpattern
alua.getpattern = _alua.channel.getpattern
alua.client = _alua.channel.client
alua.server = _alua.channel.server

return alua
