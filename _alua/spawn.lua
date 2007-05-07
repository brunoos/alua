-- $Id$
-- copyright (c) 2005 pedro martelletto <pedro@ambientworks.net>
-- all rights reserved. part of the alua project.

module("_alua.daemon.spawn", package.seeall)

require("socket")
require("posix")
require("_alua.event")
require("_alua.netio")

-- loop for the spawned process
local function spawn_loop(context, id, s2, parent)
  _alua.event.flush() -- get rid of past events states
  local alua = require("alua")
  alua.parent = parent or context.id
  alua.socket = s2
  alua.id = id
  local commands = { ["message"] = alua.incoming_msg }
  local callback = { read = _alua.netio.handler }
  _alua.event.add(s2, callback, { command_table = commands })
  alua.loop()
  os.exit()
end

-- spawns a local process
local function spawn(context, id, parent)
  local s1 = socket.bind("127.0.0.1", 0)
  local s2 = socket.connect(s1:getsockname())
  s1 = s1:accept() -- simulate what socketpair() would do
  local f = posix.fork()
  if not f then -- fork() failed
    s1:close()
    s2:close()
    return id, "error", "fork failed"
  end
  if f > 0 then
    s1:close()
    spawn_loop(context, id, s2, parent)
  end
  local _context = { id = id,
    command_table = _alua.daemon.process_command_table }
  _alua.event.add(s1, { read = _alua.netio.handler }, _context)
  _alua.daemon.processes[id] = s1
  s2:close()
  return id, "ok"
end

local function spawn_local(context, name, parent)
  if not name then
    name = _alua.daemon.get_new_process_id()
  end
  if _alua.daemon.processes[name] then
    return name, "error", "name already in use"
  end
  local id, status, e = spawn(context, name, parent)
  if status == "ok" then -- notify daemons of new process
    for v, sock in pairs(_alua.daemon.daemons) do -- don't notify ourselves
      if v ~= _alua.daemon.self.hash  then
        _alua.netio.async(sock, "notify", { id = id })
      end
    end
  end
  return id, status, e
end

local function spawn_distribute(context, reply, spawn_table, entries)
  local reply_table = { processes = {} }
  local callback = function (spawn_reply)
    for id, status in pairs(spawn_reply.processes) do
      reply_table.processes[id] = status
    end
    entries = entries - 1
    if entries == 0 then
      reply(reply_table)
    end -- time to reply
  end
  for daemon, args in pairs(spawn_table) do -- send all the spawn requests
    local sock = _alua.daemon.daemons[daemon]
    args.parent = context.id
    _alua.netio.async(sock, "spawn", args, callback)
  end
end

local function spawn_get_names(names, count)
  if not names then
    return {}, {}
  end
  local remaining = {}
  local used = {}
  local n = table.getn(names)
  for i = n, count + 1, - 1 do
    table.insert(remaining, names[i])
  end
  for i = count, 1, - 1 do
    table.insert(used, names[i])
  end
  return remaining, used
end

local function spawn_prepare_table(context, arg, reply)
  local spawn_table = {}
  local count = arg.processes
  local perdaemon = math.floor(count / _alua.daemon.ndaemons)
  local mod = math.mod(count, _alua.daemon.ndaemons)
  local entries = 0
  local remaining = arg.names
  for daemon in pairs(_alua.daemon.daemons) do
    local entry = { count = perdaemon }
    if mod > 0 then
      entry.count = entry.count + 1
      mod = mod - 1
    end
    remaining, entry.names = spawn_get_names(remaining, entry.count)
    spawn_table[daemon] = entry
    entries = entries + 1
  end
  spawn_distribute(context, reply, spawn_table, entries)
end

local function spawn_by_table(context, arg, reply)
  local spawn_table = {}
  local daemons = arg.processes
  local entries = 0
  for daemon, processes in pairs(daemons) do
    entries = entries + 1
    if type(processes) == "number" then
      spawn_table[daemon] = { count = processes, names = {} }
    else
      spawn_table[daemon] = { count = table.getn(processes),
        names = processes }
    end
  end
  spawn_distribute(context, reply, spawn_table, entries)
end

local function spawn_by_name(context, arg, reply)
  arg.names = arg.processes
  arg.processes = table.getn(arg.processes)
  spawn_prepare_table(context, arg, reply)
end

function _alua.daemon.spawn.from_process(s, context, arg, reply)
  if type(arg.processes) == "number" then
    spawn_prepare_table(context, arg, reply)
  elseif type(arg.processes) == "table" then
    if type(arg.processes[1]) == "string" then
      spawn_by_name(context, arg, reply)
    else
      spawn_by_table(context, arg, reply)
    end
  end
end

function _alua.daemon.spawn.from_daemon(sock, context, argument, reply)
  local processes = {}
  for i = argument.count, 1, - 1 do
    local id, status, e = spawn_local(context, argument.names[i],
      argument.parent)
    processes[id] = { daemon = _alua.daemon.self.hash, status = status,
      error = e }
  end
  reply({ processes = processes })
end
