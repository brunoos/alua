-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's licence can be found 
-- in the LICENCE file.
--

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

daemons = {}
ndaemons = 0
processes = {}

-- count of local processes
local idcount = 0
-- initialization variables
local pending = {}
local self_connection = "disconnected"

-- generate a new process id
function get_new_process_id()
   local id = string.format("%s:%u", self.hash, idcount)
   idcount = idcount + 1
   return id
end

-- Auxiliary funcion for syncing two daemons' processes list.
local function sync_proclist(s)
  -- Get a list of local processes.
  local lprocs = {}
  for p in pairs(processes) do
     table.insert(lprocs, p)
  end
  -- Send it to the remote daemon, and get its list back.
  local reply = _alua.netio.sync(s, "sync", { procs = lprocs })
  for _, p in pairs(reply.arguments.procs) do
     if not processes[p] then
        processes[p] = s
     end
  end
end

-- Get a connection with a daemon.
function get(daemon, callback)
   local s = daemons[daemon]
   if s then
      -- Already connected.
      if callback then
         callback(s)
      end
      return s
   end
   local s, e = socket.connect(unhash(daemon))
   if not s then
      return nil, e
   end
   s:setoption('tcp-nodelay', true)
   local ctx = { command_table = command_table, id = daemon }
   local cb = { read = _alua.netio.handler }
   _alua.event.add(s, cb, ctx)
   if callback then
      -- Async.
      local f = function (reply) callback(s) end
      _alua.netio.async(s, "auth", { mode = "daemon", id = self.hash }, f)
   else
      _alua.netio.sync(s, "auth", { mode = "daemon", id = self.hash })
      sync_proclist(s)
   end
   daemons[daemon] = s
   ndaemons = ndaemons + 1
   return s
end

-- hash an (address, port) set
function hash(addr, port)
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
function unhash(hash)
   local addr, port = string.match(hash, "(%d.+):(%d+)")
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
   local s = get(alua.id)
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
         local s = get(v)
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
   if arg.next > #arg.daemons then
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
   local s = get(arg.daemons[arg.next])
   _alua.netio.async(s, "link", arg, cb)
end

-- Send our process list to another daemon.
local function daemon_sync(s, context, arg, reply)
   local procs = {}
   for p in pairs(processes) do
      table.insert(procs, p)
   end
   for _, p in pairs(arg.procs) do
      if not processes[p] then
         processes[p] = s
      end
   end
   reply({ procs = procs })
end

-- Authenticate a remote endpoint, either as a process or a daemon.
local function proto_auth(sock, context, argument, reply)
   context.mode = argument.mode
   if argument.mode == "process" then
      context.id = get_new_process_id()
      context.command_table = process_command_table
      processes[context.id] = sock
  end
  if argument.mode == "daemon" then
     context.id = argument.id
     context.command_table = command_table
     daemons[context.id] = sock
  end

  -- If we don't have a connection to ourselves, it's a good time to get one.
  if self_connection == "connected" then
     reply({ id = context.id, daemon = self.hash })
  else
     -- We are trying to connect with ourselves?
     if context.id == self.hash then
        reply({ id = context.id, daemon = self.hash })
     else
        table.insert(pending, {reply = reply, id = context.id})
     end
  end
end

-- Dequeue an incoming connection, set it to a raw context.
function incoming_connection(sock, context)
   local incoming_sock, e = sock:accept()
   incoming_sock:setoption('tcp-nodelay', true)
   local commands = { ["auth"] = proto_auth }
   local callback = { read = _alua.netio.handler }
   _alua.event.add(incoming_sock, callback, { command_table = commands })
end

-- Create a new daemon, as requested by the user.
function create(user_conf)
   local sock, callback, h, f, e
   self = { addr = "*", port = 6080 }
   if user_conf then
      for i, v in pairs(user_conf) do
         self[i] = v
      end
   end
   sock, e = socket.bind(self.addr, self.port)
   if not sock then
      return nil, e
   end
   sock:setoption('tcp-nodelay', true)
   h = hash(sock:getsockname())
   f, e = posix.fork()
   -- fork() failed
   if not f then 
      return nil, e
   end
   -- parent
   if f > 0 then
      sock:close()
      return h
   end
   self.hash = h
   self.socket = sock
   callback = { read = incoming_connection }
   _alua.event.add(self.socket, callback)

   f = function (s)
          self_connection = "connected"
          -- internal state
          alua.socket = s
          alua.id = self.hash
          alua.daemon = self.hash
          -- reply the pending requests
          for k, v in ipairs(pending) do
             v.reply({ id = v.id, daemon = self.hash })
          end
          -- clean up
          pending = nil
       end
   get(self.hash, f)

   while true do
      _alua.event.loop()
      _alua.timer.poll()
   end
end

-- Connect to a daemon, as requested by the user.
function connect_process(daemon, auth_callback)
   local sock, e = socket.connect(unhash(daemon))
   if not sock then 
      return nil, e 
   end
   sock:setoption('tcp-nodelay', true)
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
function notify(s, context, arg, reply)
   processes[arg.id] = s
end

process_command_table = {
   ["link"] = process_link,
   ["spawn"] = _alua.spawn.from_process,
   ["message"] = _alua.message.from_process,
}

command_table = {
   ["link"] = daemon_link,
   ["sync"] = daemon_sync,
   ["spawn"] = _alua.spawn.from_daemon,
   ["notify"] = notify,
   ["message"] = _alua.message.from_daemon,
}

_alua.utils.protect(process_command_table, _alua.utils.invalid_command)
_alua.utils.protect(command_table, _alua.utils.invalid_command)
