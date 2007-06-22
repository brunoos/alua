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

-- Forward a message to a process or daemon.
local function forward(msg, header, reply)
   -- Save the timout value and do not forward it
   local timeout = header.timeout
   header.timeout = nil
   -- Find the socket to the destination
   local s = _alua.daemon.processes[header.to] or 
             _alua.daemon.daemons[header.to]
   if s then
      -- XXX LuaTimer may not be available and timer functions 
      -- may not work as expected (see module '_alua.timer')
      local newreply
      if not timeout then
         newreply = reply
      else
         local timer
         local fired = false
         local cb = function(t)
            reply({ to = header.to, status = "error", error = "timeout" })
            _alua.timer.del(timer)
            fired = true
         end
         timer = _alua.timer.add(cb, timeout)
         newreply = function(r)
            if not fired then
               _alua.timer.del(timer)
               reply(r)
            end
         end
      end
      _alua.netio.async(s, "message", header, newreply)
      s:send(msg)
   else
      reply({ to = header.to, status = "error", error = "process not found" })
   end
   -- Restore the timout value
   header.timeout = timeout
end

-- Select the message destination.
local function unicast(msg, header, reply)
   -- See if it's a message for us.
   if header.to == alua.id then
      alua.execute(msg, reply)
   else
      forward(msg, header, reply)
   end
end

local function multicast(msg, header, reply)
   local to = {}
   local count = 0

   -- Take only unique identifiers
   local tmp = {}
   for k, v in ipairs(header.to) do
      if not tmp[v] then
         tmp[v] = true
         table.insert(to, v)
         count = count + 1
      end
   end

   local answers = {}
   local newreply = function(m)
      answers[m.to] = { status = m.status, error = m.error }
      count = count - 1
      if count == 0 then
         reply(answers)
      end
   end

   local backup = header.to
   for k, v in ipairs(to) do
      header.to = v
      unicast(msg, header, newreply)
   end
   header.to = backup
end

-- Daemon handler for the 'message' request.
local function process(sock, header, reply, isprocess)
   local msg = sock:receive(header.len)
   if type(header.to) == "table" then
      multicast(msg, header, reply)
   else
      local newreply = reply
      -- If the request comes from a process, transform the reply
      if isprocess then
         local answer = {}
         newreply = function(m)
            answer[m.to] = { status = m.status, error = m.error }
            reply(answer)
         end
      else
         -- Else, keep the original reply
         newreply = reply
      end
      unicast(msg, header, newreply)
   end
end

function from_process(sock, context, header, reply)
   process(sock, header, reply, true)
end

function from_daemon(sock, context, header, reply)
   -- Since the daemon is a process too, check who is sending the request.
   local isprocess = ((alua.id == context.id) and true) or false
   process(sock, header, reply, isprocess)
end
