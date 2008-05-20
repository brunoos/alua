-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's license can be found 
-- in the LICENSE file.
--

module("alua.daemon.message", package.seeall)

-- Standard modules
require("table")
require("string")
-- Internal modules
require("alua.event")
require("alua.daemon.context")

-- Alias
local context = alua.daemon.context

--
-- Try to resolve the alias and send it the message.
--
local function resolve(msg, reply)
   local conn
   local count = 0
   local ambiguous = false
   -- Handle the resolution replies
   local cb = function(m)
      count = count - 1
      -- Ambiguous resolution already detected, discarte all replies
      if ambiguous then
         return
      elseif m.status == "ok" and m.exists then
         if conn then
            ambiguous = true
            reply({status = "error", error = "ambiguous name resolution"})
            return
         else
            conn = context.dmn_getconn(m.daemon)
         end
      end
      if count == 0 then
         if conn then
            context.als_save(msg.dst, conn)
            alua.event.send(conn, "message", msg, reply)
         else
            reply({status = "error", error = "unknown process"})
         end
      end
   end
   -- Send the resolution request
   for i, c in context.dmn_iter() do
      count = count + 1
      alua.event.send(c, "exists", {process = msg.dst}, cb)
   end
   if count == 0 then
      reply({status = "error", error = "unknown process"})
   end
end

--
-- Route a message from a client to a daemon or another client.
--
local function unicast(msg, reply, from)
   local id = msg.dst
   -- it is a known process -> just send the message
   local conn = context.prc_getconn(id)
   if conn then
      alua.event.send(conn, "message", msg, reply)
   else
      -- it is a known daemon -> just send the message
      conn = context.dmn_getconn(id)
      if conn then
         alua.event.send(conn, "message", msg, reply)
      else
         -- try to find the daemon in charge of it.
         local daemon = string.match(id, "^%d+@(%d+%.%d+%.%d+%.%d+:%d+)$")
         if daemon and daemon == alua.id then
            reply({status = "error", error = "unknown process"})
         elseif daemon then
            conn = context.dmn_getconn(daemon)
            if conn then
               alua.event.send(conn, "message", msg, reply)
            else
               reply({status = "error", error = "unknown daemon"})
            end
         else
            -- It is not an internal identification, i.e., it is a given name
            -- from the user (alias).
            conn = context.als_getconn(id)
            if conn then
               alua.event.send(conn, "message", msg, reply)

            -- Unknown alias, but only try to resolve it if the request
            -- came from a process in order to avoid creating infinity 
            -- resolution requests.
            elseif context.prc_getid(from) ~= nil then
               resolve(msg, reply)
            else
               reply({status = "error", error = "unknown process"})
            end
         end
      end
   end
end

--
-- Send a message to an array of processes/daemons.
--
local function multicast(msg, reply, from)
   -- Keep track of each name and it reply using closures
   local resp = { }
   local nresp = #msg.dst
   local aux = function(dst, m)
      resp[dst] = m
      nresp = nresp - 1
      if nresp == 0 then
         reply(resp)
      end
   end
   for k, v in ipairs(msg.dst) do
      local cb = function(m)
         aux(v, m)
      end
      msg.dst = v
      unicast(msg, cb, from)
   end
end

--
-- Send a message to other process or daemon.
--
function evt_message(msg, reply, from)
   if type(msg.dst) == "string" then
      unicast(msg, reply, from)
   elseif type(msg.dst) == "table" then
      multicast(msg, reply, from)
   else
      reply({status = "error", error = "invalid request"})
   end
end
