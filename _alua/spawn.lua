-- $Id$
-- copyright (c) 2005 pedro martelletto <pedro@ambientworks.net>
-- all rights reserved. part of the alua project.

module("_alua.spawn", package.seeall)

require("socket")
require("posix")
require("_alua.event")
require("_alua.netio")

----
-- First part: organize and send the request to daemons
--

local function spawn_distribute(context, reply, spawn_table, entries)
   local reply_table = { processes = {}, status = "ok" }
   local callback = 
      function (spawn_reply)
         for id, status in pairs(spawn_reply.processes) do
            reply_table.processes[id] = status
         end
         entries = entries - 1
         -- time to reply
         if entries == 0 then
            reply(reply_table)
         end
      end
   -- send all the spawn requests
   for daemon, args in pairs(spawn_table) do
      local sock = _alua.daemon.daemons[daemon]
      args.parent = context.id
      _alua.netio.async(sock, "spawn", args, callback)
   end
end

local function spawn_get_names(names, count)
   if not names then
      return nil, {}
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
   local remaining = arg.names
   for daemon in pairs(_alua.daemon.daemons) do
      local entry = { count = perdaemon }
      if mod > 0 then
         entry.count = entry.count + 1
         mod = mod - 1
      end
      remaining, entry.names = spawn_get_names(remaining, entry.count)
      spawn_table[daemon] = entry
   end
   spawn_distribute(context, reply, spawn_table, _alua.daemon.ndaemons)
end

local function spawn_by_table(context, arg, reply)
   local spawn_table = {}
   local daemons = arg.processes
   local entries = 0
   for daemon, processes in pairs(daemons) do
      if not _alua.daemon.daemons[daemon] then
         local msg = "daemon " .. daemon .. " not found"
         reply({status = "error", error = msg})
         return
      end
      if type(processes) == "number" then
         spawn_table[daemon] = { count = processes, names = {} }
      else
         spawn_table[daemon] = { 
            count = table.getn(processes),
            names = processes 
         }
      end
      entries = entries + 1
   end
   spawn_distribute(context, reply, spawn_table, entries)
end

local function spawn_by_name(context, arg, reply)
   arg.names = arg.processes
   arg.processes = table.getn(arg.processes)
   spawn_prepare_table(context, arg, reply)
end

function _alua.spawn.from_process(s, context, arg, reply)
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


----
-- Second part: start new processes and send reply
--

-- loop for the spawned process
local function spawn_loop(context, id, s, parent)
   -- get rid of past events states
   _alua.event.flush()
   local alua = require("alua")
   alua.parent = parent or context.id
   alua.socket = s
   alua.id = id
   local commands = { ["message"] = alua.incoming_msg }
   local callback = { read = _alua.netio.handler }
   _alua.event.add(s, callback, { command_table = commands })
   alua.loop()
   os.exit()
end

-- spawns a local process
local function spawn(context, id, parent)
   -- simulate what socketpair() would do
   local s1 = socket.bind("127.0.0.1", 0)
   local s2 = socket.connect(s1:getsockname())
   local s3, err = s1:accept()
   if not s3 then
      s1:close()
      s2:close()
      return "error", "spawn failed"
   end
   s1:close()

   local f = posix.fork()
   -- fork() failed
   if not f then
      s2:close()
      s3:close()
      return "error", "fork failed"
   end
   if f > 0 then
      s3:close()
      spawn_loop(context, id, s2, parent)
   else
      s2:close()
      local _context = { 
         id = id,
         command_table = _alua.daemon.process_command_table 
      }
      _alua.event.add(s3, { read = _alua.netio.handler }, _context)
      _alua.daemon.processes[id] = s3
      return "ok"
   end
   return "error", "what are you doing here?"
end

local function spawn_local(context, name, parent)
   if not name then
      name = _alua.daemon.get_new_process_id()
   end
   if _alua.daemon.processes[name] then
      return name, "error", "name already in use"
   end
   local status, e = spawn(context, name, parent)
   -- notify daemons of new process
   if status == "ok" then
      -- don't notify ourselves
      for v, sock in pairs(_alua.daemon.daemons) do
         if v ~= _alua.daemon.self.hash  then
            _alua.netio.async(sock, "notify", { id = name })
         end
      end
   end
   return name, status, e
end

function _alua.spawn.from_daemon(sock, context, argument, reply)
   local processes = {}
   for i = argument.count, 1, - 1 do
      local id, status, e = spawn_local(context, argument.names[i],
         argument.parent)
      processes[id] = { 
         daemon = _alua.daemon.self.hash, 
         status = status,
         error = e 
      }
  end
  reply({ processes = processes })
end
