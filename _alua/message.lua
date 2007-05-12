-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's licence can be found 
-- in the LICENCE file.
--

module("_alua.message", package.seeall)

require("_alua.netio")
require("_alua.timer")
require("_alua.daemon")

-- Deliver a message to a process.
local function msg_deliver(context, header, msg, callback)
   -- Save the timout value and do not forward it
   local timeout = header.timeout
   header.timeout = nil
   local to = header.to
   local s = _alua.daemon.processes[to] or _alua.daemon.daemons[to]
   if s then
      local f = callback
      -- XXX LuaTimer may not be available and timer functions 
      -- may not work as expected (see _alua.timer module)
      if timeout then
         local timer
         local fired = false
         local cb = function(t)
                callback({ to = to, status = "error", error = "timeout" })
                _alua.timer.del(timer)
                fired = true
             end
         timer = _alua.timer.add(cb, timeout)
         f = function(reply)
                if not fired then
                   _alua.timer.del(timer)
                   callback(reply)
                end
             end
      end
      _alua.netio.async(s, "message", header, f)
      s:send(msg)
   else
      callback({ to = to, status = "error", error = "process not found" })
   end
   -- Restore the timout value
   header.timeout = timeout
end

-- Receive a message from a process and forward it.
local function message_common(sock, context, header, reply, forwarding)
   local msg, e = sock:receive(header.len)
   if not forwarding and type(header.to) == "table" then
      -- Save the target address
      local to = header.to
      for _, dest in pairs(to) do 
        header.to = dest
        msg_deliver(context, header, msg, reply)
     end
     -- Restore the target address
     header.to = to
   else
      msg_deliver(context, header, msg, reply)
   end
end

-- Process handler for the 'message' request.
function from_process(sock, context, header, reply)
   local _reply = {}
   local count = type(header.to) == "table" and #header.to or 1
   local reply_callback = 
      function (msg)
         _reply[msg.to] = { status = msg.status, error = msg.error }
         count = count - 1
         if count == 0 then
            reply(_reply)
         end
      end
   -- See if it's a message for us.
   if header.to == alua.id then
      alua.incoming_msg(sock, context, header, reply_callback)
   else
      message_common(sock, context, header, reply_callback, false)
   end
end

-- Daemon handler for the 'message' request.
function from_daemon(sock, context, header, reply)
   -- See if it's a message for us.
   if header.to == alua.id then
      alua.incoming_msg(sock, context, header, reply)
   else
      message_common(sock, context, header, reply, true)
   end
end
