-- $Id$
-- copyright (c) 2005 pedro martelletto <pedro@ambientworks.net>
-- all rights reserved. part of the alua project.

module("_alua.daemon", package.seeall)

-- external modules
require("socket")
require("posix")

-- internal modules
require("_alua.event")
require("_alua.netio")
require("_alua.utils")
require("_alua.spawn")
require("_alua.message")
require("_alua.timer")

_alua.daemon.daemons = {}
_alua.daemon.ndaemons = 0
_alua.daemon.processes = {}

-- count of local processes
local idcount = 0
-- initialization variables
local pending_replies = {}
local self_connection_state = "disconnected"

-- generate a new process id
function _alua.daemon.get_new_process_id()
   local id = string.format("%s:%u", _alua.daemon.self.hash, idcount)
   idcount = idcount + 1
   return id
end

-- Auxiliary funcion for syncing two daemons' processes list.
local function sync_proclist(s)
  -- Get a list of local processes.
  local lprocs = {}
  for p in pairs(_alua.daemon.processes) do
     table.insert(lprocs, p)
  end
  -- Send it to the remote daemon, and get its list back.
  local reply = _alua.netio.sync(s, "sync", { procs = lprocs })
  for _, p in pairs(reply.arguments.procs) do
     if not _alua.daemon.processes[p] then
        _alua.daemon.processes[p] = s
     end
  end
end

-- Get a connection with a daemon.
function _alua.daemon.get(hash, callback)
   local s = _alua.daemon.daemons[hash]
   if s then
      -- Already connected.
      if callback then
         callback(s)
      end
      return s
   end
   local s, e = socket.connect(_alua.daemon.unhash(hash))
   if not s then
      return nil, e
   end
   local _context = { command_table = _alua.daemon.command_table }
   local _callback = { read = _alua.netio.handler }
   _alua.event.add(s, _callback, _context)
   if callback then
      -- Async.
      local f = function (reply)
                   callback(s)
                end
      _alua.netio.async(s, "auth", { mode = "daemon",
                           id = _alua.daemon.self.hash }, f)
   else
      _alua.netio.sync(s, "auth", { mode = "daemon",
                          id = _alua.daemon.self.hash })
      sync_proclist(s)
   end
   _alua.daemon.daemons[hash] = s
   _alua.daemon.ndaemons = _alua.daemon.ndaemons + 1
   return s
end

-- hash an (address, port) set
function _alua.daemon.hash(addr, port)
   -- workaround
   if addr == "0.0.0.0" then 
      addr = "127.0.0.1" 
      -- Try to query DNS about an IP different from 127.0.0.1
      local name = socket.dns.gethostname()
      if name then
         local ip, info = socket.dns.toip(name)
         if ip then
            for k, v in pairs(info.ip) do
               if v ~= "127.0.0.1" then
                  addr = v
                  break
               end
            end
         end
      end
   end
   return string.format("%s:%u", addr, port)
end

-- unhash a (address, port) set
function _alua.daemon.unhash(hash)
   local _, _, addr, port = string.find(hash, "(%d.+):(%d+)")
   return addr, tonumber(port)
end

-- Extend our network of daemons.
local function process_link(s, context, arg, reply)
   local tmp = {}
   local daemons = {}
   local myself = false

   -- Remove the repeated identifications
   for k, v in ipairs(arg.daemons) do
      if alua.id ~= v then
         if not tmp[v] then
            tmp[v] = true
            table.insert(daemons, v)
         end
      else
         -- The current daemon is in the list
         myself = true
      end
   end

   -- Insert the current daemon into the list in the first position
   table.insert(daemons, 1, alua.id)

   -- Link was done, request the next daemon to make the links
   local arg = { daemons = daemons, next = 1 }

   -- The current daemon is not in the original list, 
   -- do not reply it
   if not myself then
      arg.exclude = alua.id
   end

   local cb = function(arg)
                 reply(arg)
              end
   local s = _alua.daemon.get(alua.id)
   _alua.netio.async(s, "link", arg, cb)
end

-- Extend our network of daemons, request coming from a daemon.
local function daemon_link(s, context, arg, reply)
   -- Set all connections as fail before connect the daemons
   local daemons = {}

   for k, v in ipairs(arg.daemons) do
      daemons[v] = "unknown"
   end

   -- Try to connect the daemons
   local fail = false
   for k, v in ipairs(arg.daemons) do
      if alua.id ~= v then
         local s = _alua.daemon.get(v)
         if not s then
            fail = true
            daemons[v] = "fail"
            break
         else
            daemons[v] = "ok"
         end
      else
         daemons[v] = "ok"
      end
   end

   -- At least a connection fail -> return error
   if fail then
      if arg.exclude then
         daemons[arg.exclude] = nil
      end
      local tmp = { status = "error", daemons = daemons}
      tmp.error = alua.id .. ": link failure"
      reply(tmp)
      return
   end

   -- If we are the last daemon, reply successfully
   arg.next = arg.next + 1
   if arg.next > table.getn(arg.daemons) then
      if arg.exclude then
         daemons[arg.exclude] = nil
      end
      reply({ status = "ok", daemons = daemons })
      return
   end

   -- We are not the last daemon, request the next daemon to 
   -- make the links
   local cb = function(arg)
                 reply(arg)
              end
   local s = _alua.daemon.get(arg.daemons[arg.next])
   _alua.netio.async(s, "link", arg, cb)
end

-- Send our process list to another daemon.
local function daemon_sync(s, context, arg, reply)
   local procs = {}
   for p in pairs(_alua.daemon.processes) do
      table.insert(procs, p)
   end
   for _, p in pairs(arg.procs) do
      if not _alua.daemon.processes[p] then
         _alua.daemon.processes[p] = s
      end
   end
   reply({ procs = procs })
end

-- Authenticate a remote endpoint, either as a process or a daemon.
local function proto_auth(sock, context, argument, reply)
   context.mode = argument.mode
   if argument.mode == "process" then
      context.id = _alua.daemon.get_new_process_id()
      context.command_table = _alua.daemon.process_command_table
      _alua.daemon.processes[context.id] = sock
  end
  if argument.mode == "daemon" then
     context.id = argument.id
     context.command_table = _alua.daemon.command_table
     _alua.daemon.daemons[context.id] = sock
  end

  -- If we don't have a connection to ourselves, it's a good time to get one.
  if self_connection_state == "connected" then
     reply({ id = context.id, daemon = _alua.daemon.self.hash })
  else
     -- We are trying to connect with ourselves?
     if context.id == _alua.daemon.self.hash then
        reply({ id = context.id, daemon = _alua.daemon.self.hash })
     else
        table.insert(pending_replies, {reply = reply, id = context.id})
     end
  end
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
   local sock, callback, hash, f, e
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
   hash = _alua.daemon.hash(sock:getsockname())
   f, e = posix.fork()
   -- fork() failed
   if not f then 
      return nil, e
   end
   -- parent
   if f > 0 then
      sock:close()
      return hash
   end
   _alua.daemon.self.hash = hash
   _alua.daemon.self.socket = sock
   callback = { read = _alua.daemon.incoming_connection }
   _alua.event.add(_alua.daemon.self.socket, callback)

   f = function (s)
          self_connection_state = "connected"
          -- internal state
          alua.socket = s
          alua.id = _alua.daemon.self.hash
          alua.daemon = _alua.daemon.self.hash
          -- reply the pending requests
          for k, v in ipairs(pending_replies) do
             v.reply({ id = v.id, daemon = _alua.daemon.self.hash })
          end
          -- clean up
          pending_replies = nil
       end
   _alua.daemon.get(_alua.daemon.self.hash, f)

   while true do
      _alua.event.loop()
      _alua.timer.poll()
   end
end

-- Connect to a daemon, as requested by the user.
function _alua.daemon.connect_process(daemon, auth_callback)
   local sock, e = socket.connect(_alua.daemon.unhash(daemon))
   if not sock then 
      return nil, e 
   end
   local reply, e = _alua.netio.sync(sock, "auth", { mode = "process" })
   if not reply then 
      return nil, e 
   end
   return {
     socket = sock, 
     id = reply.arguments.id,
     daemon = reply.arguments.daemon,
   }
end

-- Get information about an incoming/leaving process.
function _alua.daemon.notify(s, context, arg, reply)
   _alua.daemon.processes[arg.id] = s
end

_alua.daemon.process_command_table = {
   ["link"] = process_link,
   ["spawn"] = _alua.spawn.from_process,
   ["message"] = _alua.message.from_process,
}

_alua.daemon.command_table = {
   ["link"] = daemon_link,
   ["sync"] = daemon_sync,
   ["spawn"] = _alua.spawn.from_daemon,
   ["notify"] = _alua.daemon.notify,
   ["message"] = _alua.message.from_daemon,
}

_alua.utils.protect(_alua.daemon.process_command_table,
                    _alua.utils.invalid_command)
_alua.utils.protect(_alua.daemon.command_table,
                    _alua.utils.invalid_command)
