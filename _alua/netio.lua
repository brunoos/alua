-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's licence can be found 
-- in the LICENCE file.
--

module("_alua.netio", package.seeall)

require("_alua.event")
require("_alua.utils")

-- Transform an outgoing '<mode>, <header>, <arguments>' tuple into a string
-- and send it on the given socket
function send(sock, mode, header, data)
   sock:send(string.format("%s = %s, arguments = %s\n", mode,
      _alua.utils.dump(header), _alua.utils.dump(data)))
end

-- Receive and parse incoming data into a '<command>, <arguments>' pair
function recv(sock)
   local data, e = sock:receive()
   if not data then return nil end
   -- Load the received chunk
   data = "return { " .. data .. " }" 
   local f, e = loadstring(data)
   if not f then 
      return nil 
   end
   setfenv(f, {})
   return f()
end

-- Handle an incoming request. try to find a handler for it, and prepare a
-- reply function to be used by the handler
function request(sock, context, incoming)
   local id = incoming.request.id 
   local name = incoming.request.name
   local args = incoming.arguments
   local reply = 
      function (body)
         send(sock,  "reply", { id = id }, body)
      end
   local handler = context.command_table[name]
   handler(sock, context, args, reply)
end

-- Handle an incoming reply
function reply(sock, context, incoming)
   local id = incoming.reply.id
   local arguments = incoming.arguments
   local callback = context.reqtable[id]
   -- Remove from sequence table
   context.reqtable[id] = nil
   if callback then
      callback(arguments)
   end
end

-- Generic event handling function
function handler(sock, context)
   local incoming = recv(sock) or {}
   if incoming.request then
      request(sock, context, incoming)
   elseif incoming.reply then 
      reply(sock, context, incoming)
   else 
      _alua.event.del(sock)
   end
end

-- Issue a protocol command, asynchronously
function async(sock, cmd, arg, callback)
   local _, context = _alua.event.get(sock)
   -- Arrange sequence table
   context.reqtable = context.reqtable or {}
   -- Tag the request
   local id = #context.reqtable + 1
   local req = { name = cmd, id = id }
   context.reqtable[id] = callback
   send(sock, "request", req, arg)
end

-- Issue a protocol command, synchronously
function sync(sock, cmd, args)
   send(sock, "request", { name = cmd }, args)
   return recv(sock)
end
