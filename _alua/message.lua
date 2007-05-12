-- $Id$
-- copyright (c) 2005 pedro martelletto <pedro@ambientworks.net>
-- all rights reserved. part of the alua project.

module("_alua.message", package.seeall)

require("_alua.netio")
require("_alua.timer")
require("_alua.daemon")

-- Deliver a message to a process.
local function msg_deliver(context, header, msg, callback)
   local to = header.to
   local s = _alua.daemon.processes[to] or _alua.daemon.daemons[to]
   if s then
      local timer, f
      if header.timeout then
        f = function(t)
              callback({ to = to, status = "error", error = "timeout" })
              _alua.timer.del(t)
            end
         timer = _alua.timer.add(f, header.timeout)
      end
      f = function(reply)
            callback(reply)
            if timer then
              _alua.timer.del(timer)
            end
         end
      _alua.netio.async(s, "message", header, f)
      s:send(msg)
   else
      callback({ to = to, status = "error", error = "process not found" })
   end
end

-- Receive a message from a process and forward it.
local function message_common(sock, context, header, reply, forwarding)
   local msg, e = sock:receive(header.len)
   if not forwarding and type(header.to) == "table" then
      -- fake new header
      local to = header.to
      for _, dest in pairs(to) do 
        header.to = dest
        msg_deliver(context, header, msg, reply)
      end
      header.to = to
   else
      msg_deliver(context, header, msg, reply)
   end
end

-- Process handler for the 'message' request.
function _alua.message.from_process(sock, context, header, reply)
   local _reply = {}
   local count = type(header.to) == "table" and table.getn(header.to) or 1
   local reply_callback = 
      function (msg)
         _reply[msg.to] = { status = msg.status, error = msg.error }
         count = count - 1
         if count == 0 then
            reply(_reply)
         end
      end
   -- See if it's a message for us.
   if header.to == _alua.daemon.self.hash then
      alua.incoming_msg(sock, context, header, reply_callback)
   else
      message_common(sock, context, header, reply_callback, false)
   end
end

-- Daemon handler for the 'message' request.
function _alua.message.from_daemon(sock, context, header, reply)
   -- See if it's a message for us.
   if header.to == _alua.daemon.self.hash then
      alua.incoming_msg(sock, context, header, reply)
   else
      message_common(sock, context, header, reply, true)
   end
end
