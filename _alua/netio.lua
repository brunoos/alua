-- $Id$
-- copyright (c) 2005 pedro martelletto <pedro@ambientworks.net>
-- all rights reserved. part of the alua project.

module("_alua.netio", package.seeall)

require("_alua.event")
require("_alua.utils")

-- transform an outgoing '<mode>, <header>, <arguments>' tuple into a string
-- and send it on the given socket
function _alua.netio.send(sock, mode, header, data)
   sock:send(string.format("%s = %s, arguments = %s\n", mode,
      _alua.utils.dump(header), _alua.utils.dump(data)))
end

-- receive and parse incoming data into a '<command>, <arguments>' pair
function _alua.netio.recv(sock)
   local data, e = sock:receive()
   if not data then return nil end
   -- load the received chunk
   data = "return { " .. data .. " }" 
   local f, e = loadstring(data)
   if not f then 
      return nil 
   end
   setfenv(f, {})
   return f()
end

-- handle an incoming request. try to find a handler for it, and prepare a
-- reply function to be used by the handler
function _alua.netio.request(sock, context, incoming)
   local request, arguments = incoming.request, incoming.arguments
   local reply = 
      function (body)
         _alua.netio.send(sock,  "reply", { id = request.id }, body)
      end
   local handler = context.command_table[request.name]
   handler(sock, context, arguments, reply)
end

-- handle an incoming reply
function _alua.netio.reply(sock, context, incoming)
   local reply, arguments = incoming.reply, incoming.arguments
   local callback = context.reqtable[reply.id]
   -- remove from sequence table
   context.reqtable[reply.id] = nil
   if reply.id == context.reqcount - 1 then
      context.reqcount = reply.id - 1
   end
   if callback then
      callback(arguments)
   end
end

-- generic event handling function
function _alua.netio.handler(sock, context)
   local incoming = _alua.netio.recv(sock) or {}
   if incoming.request then
      _alua.netio.request(sock, context, incoming)
   elseif incoming.reply then 
      _alua.netio.reply(sock, context, incoming)
   else 
      _alua.event.del(sock)
   end
end

-- issue a protocol command, asynchronously
function _alua.netio.async(sock, cmd, arg, callback)
   local _, context = _alua.event.get(sock)
   -- arrange sequence record
   context.reqcount = context.reqcount or 0
   -- arrange sequence table
   context.reqtable = context.reqtable or {}
   -- tag the request
   local req = { name = cmd, id = context.reqcount }
   context.reqtable[context.reqcount] = callback
   context.reqcount = context.reqcount + 1
   _alua.netio.send(sock, "request", req, arg)
end

-- issue a protocol command, synchronously
function _alua.netio.sync(sock, cmd, args)
   _alua.netio.send(sock, "request", { name = cmd }, args)
   return _alua.netio.recv(sock)
end
