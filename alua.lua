-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's licence can be found 
-- in the LICENCE file.
--

alua = {}
package.loaded["alua"] = alua

require("_alua.event")
require("_alua.netio")
require("_alua.utils")
require("_alua.timer")
require("_alua.daemon")
require("_alua.channel")

-- handler for incoming daemon messages
function alua.incoming_msg(sock, context, header, reply)
   -- Receive the message from the daemon
   local msg, e = sock:receive(header.len)
   if not msg then 
      alua.close()
      return 
   else
      alua.execute(msg, reply)
   end
end

-- Execute a message. This function is shared: both daemon and process use it.
function alua.execute(msg, reply)
   -- Load message into executable object
   local obj = loadstring(msg)
   -- If we exit(), reply first
   local _exit = os.exit
   os.exit = function(code)
      reply({ to = alua.id, status = "ok" })
      _exit(code)
   end 
   -- Execute the message
   local okay, e = pcall(obj)
   okay = (okay and "ok") or "error"
   os.exit = _exit
   reply({ to = alua.id, status = okay, error = e })
end

-- issue commands to the daemon
function alua.command(type, arg, callback)
   if not alua.socket then -- error out
      if callback then 
         callback({ status = "error", error = "not connected" }) 
      end
   else 
      _alua.netio.async(alua.socket, type, arg, callback) 
   end
end

-- main event loop
function alua.loop()
   while true do 
      _alua.event.loop()
      _alua.timer.poll()
   end
end

-- terminate a (set of) process(es)
function alua.exit(to, code, callback)
   if not to then 
      os.exit(code) 
   end
   code = code or "nil"
   alua.send(to, "alua.exit(nil, " .. code .. ")", callback)
end

-- link daemons to daemons
function alua.link(daemons, callback)
   alua.command("link", { daemons = daemons }, callback)
end

-- send a message to a (set of) process(es)
function alua.send(to, msg, callback, timeout)
   local arg = {
      to = to,
      timeout = timeout,
      len = string.len(msg),
   }
   alua.command("message", arg, callback)
   alua.socket:send(msg)
end

-- Spawn new processes.
function alua.spawn(processes, callback)
   alua.command("spawn", { processes = processes }, callback)
end

-- connect to a daemon. operates synchronously
function alua.connect(daemon)
   if alua.socket then 
      return nil, "already connected" 
   end
   local conn, e = _alua.daemon.connect_process(daemon)
   if not conn then 
      return nil, e 
   end
   alua.id = conn.id
   alua.socket = conn.socket
   alua.daemon = conn.daemon
   local commands = { ["message"] = alua.incoming_msg }
   local callback = { read = _alua.netio.handler }
   _alua.utils.protect(commands, _alua.utils.invalid_command)
   _alua.event.add(alua.socket, callback, { command_table = commands })
   return alua.daemon
end

-- open a connection with, or create a new a daemon
function alua.open(arg)
   local daemon, e
   if not arg or type(arg) == "table" then
      daemon, e = _alua.daemon.create(arg)
      if not daemon then 
         return nil, e 
      end
   end
   return alua.connect(daemon or arg)
end

-- close the connection with the current daemon
function alua.close(arg)
   if arg then 
      return _alua.channel.close(arg) 
   end
   if not alua.socket then 
      return nil, "not connected" 
   end
   _alua.event.del(alua.socket)
   alua.socket = nil
   alua.daemon = nil
   alua.id = nil
end

-- prepare the 'alua' table
alua.create = _alua.daemon.create
alua.tostring = _alua.utils.dump
-- provide simple shells for the timer functions
alua.timeradd = _alua.timer.add
alua.timerdel = _alua.timer.del
-- provide simple shells for the channel functions
alua.setpattern = _alua.channel.setpattern
alua.getpattern = _alua.channel.getpattern
alua.client = _alua.channel.client
alua.server = _alua.channel.server

return alua
