-- $Id$
-- copyright (c) 2005 pedro martelletto <pedro@ambientworks.net>
-- all rights reserved. part of the alua project.

module("_alua.message", package.seeall)

require("_alua.netio")
require("_alua.timer")

-- Deliver a message to a process.
local function msg_deliver(context, header, msg, callback)
  local to = header.to
  local s = _alua.daemon.processes[to]
  if s then
    local timer 
    if header.timeout then
      timer = _alua.timer.add(function(t)
         callback({ to = to, status = "error", error = "timeout" })
         _alua.timer.del(t)
      end, header.timeout)
    end
    _alua.netio.async(s, "message", header, function(reply)
       callback(reply)
       if timer then
          _alua.timer.del(timer)
       end
    end)
    s:send(msg)
  else
    callback({ to = to, status = "error", error = "process not found" })
  end
end

-- Receive a message from a process and forward it.
local function message_common(sock, context, header, reply, forwarding)
  local msg, e = sock:receive(header.len)
  if not header.from then
    header.from = context.id
  end
  if type(header.to) == "table" and not forwarding then
    for _, dest in pairs(header.to) do -- fake new header
      local newheader = header
      newheader.to = dest
      msg_deliver(context, newheader, msg, reply)
    end
  else
    msg_deliver(context, header, msg, reply)
  end
end

-- Process handler for the 'message' request.
function _alua.message.from_process(sock, context, header, reply)
  local done, _reply, to = {}, {}, header.to
  local count = type(to) == "table" and table.getn(to) or 1
  local reply_callback = function (msg)
    _reply[msg.to] = { status = msg.status, error = msg.error }
    count = count - 1
    if count == 0 then
      reply(_reply)
    end
  end
  message_common(sock, context, header, reply_callback, false)
end

-- Daemon handler for the 'message' request.
function _alua.message.from_daemon(sock, context, header, reply)
  -- See if it's a message for us.
  if header.to == _alua.daemon.self.hash then
    alua.incoming_msg(sock, context, header, reply)
  else
    local reply_callback = function (__reply)
      reply(__reply)
    end
    message_common(sock, context, header, reply_callback, true)
  end
end
