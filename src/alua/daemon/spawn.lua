-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's licence can be found 
-- in the LICENCE file.
--

module("alua.daemon.spawn", package.seeall)

-- Standard modules
require("math")
require("table")
require("string")
-- Internal modules
require("alua.config")
require("alua.core")
require("alua.event")
require("alua.daemon.context")

-- Alias
local context = alua.daemon.context

-- Information about the pending spawned processes connection
local pending = { }

--
-- Authenticate a new spawned process.
--
local function auth_spawn(msg, reply, conn)
   local ctx = pending[1]
   if ctx then
      local id, finished, err
      if ctx.count then
         id = tostring(context.nextidx()) .. "@" .. alua.id
         ctx.count = ctx.count - 1
         finished = (ctx.count == 0)
      else
         id = ctx.names[1]
         table.remove(ctx.names, 1)
         finished = (#ctx.names == 0)
         err = (context.prc_getconn(id) ~= nil)
      end
      if err then
         err = {
            status = "error", 
            error = "process name already exists",
         }
         reply(err)
         err.daemon = alua.id
         ctx.processes[id] = err
      else
         context.prc_save(id, conn)
         alua.event.flush(conn)
         alua.event.add(conn, context.events.process)
         reply({status = "ok", id = id, daemon = alua.id})
         ctx.processes[id] = {status = "ok", daemon = alua.id}
      end
      if finished then
         table.remove(pending, 1)
         ctx.reply({status="ok", processes = ctx.processes})
      end
   else
      reply({status = "error", error = "invalid request"})
   end
end

--
-- Register the authentication mode for spawned processes
--
context.auth["spawn"] = auth_spawn

---------------------------------------------------------------------------

--
-- Create the new process.
--
local function dospawn(msg, reply)
   -- 'count' and 'names' are mutual excludents
   if (msg.count and not msg.names) or (not msg.count and msg.names) then
      local ctx = {
         reply = reply, 
         processes = {},
         count = msg.count,
         names = msg.names,
      }
      local count = msg.count or #msg.names
      table.insert(pending, ctx)
      local cmd = string.format("alua.process.launch('%s:%d')", 
         context.serverconn.addr, 
         context.serverconn.port)
      local lua = alua.config.lua
      for i = 1, count do
         alua.core.execute(lua, "-l", "alua", "-e", cmd)
      end
   else
      reply({status = "error", error = "invalid request"})
   end
end

--
-- Spawn new 'number' processes.
-- This function divides the processes among the daemons.
--
local function spawnbynumber(msg, reply)
   if msg.count <= 0 then
      reply({status = "error", error = "invalid number of process"})
      return
   end
   local total = context.dmn_count + 1
   local each = math.floor(msg.count/total)
   local remain = msg.count - (each*total)
   -- Callback to receive the daemons reply
   local resp = { }
   local nresp
   if each > 0 then
      nresp = total
   else
      nresp = remain
   end
   local cb = function(m)
      if m.status == 'ok' then
         for k, v in pairs(m.processes) do
            resp[k] = v
         end
      end
      nresp = nresp - 1
      if nresp == 0 then
         -- Not empty
         if next(resp) then  
            reply({status = "ok", processes = resp})
         else
            reply({status = "error", error = "cannot spawn the processes"})
         end
      end
   end
   -- Save my quote in order to create local processes
   local myquote = each
   if remain > 0 then
      myquote = myquote + 1
      remain = remain - 1
   end
   -- Request process spawning
   msg["local"] = true
   for id, conn in context.dmn_iter() do
      if remain > 0 then
         msg.count = each + 1
         remain = remain - 1
         alua.event.send(conn, "spawn", msg, cb)
      elseif each > 0 then
         msg.count = each
         alua.event.send(conn, "spawn", msg, cb)
      else
         break
      end
   end
   msg.count = myquote
   dospawn(msg, cb)
end

--
-- This function receives an array of names and creates 
-- new processes, dividing them among the daemons.
--
local function spawnbyname(msg, reply)
   local names = msg.names
   local count = #names
   local total = context.dmn_count + 1
   local each = math.floor(count/total)
   local remain = count - (each*total)
   -- Callback to receive the daemons reply
   local resp = { }
   local nresp
   if each > 0 then
      nresp = total
   else
      nresp = remain
   end
   local cb = function(m)
      if m.status == 'ok' then
         for k, v in pairs(m.processes) do
            resp[k] = v
         end
      end
      nresp = nresp - 1
      if nresp == 0 then
         -- Not empty
         if next(resp) then  
            reply({status = "ok", processes = resp})
         else
            reply({status = "error", error = "cannot spawn the processes"})
         end
      end
   end
   -- Save my names in order to create local processes
   local mynames = { }
   local i = 1
   local num = each
   if remain > 0 then
      num = num + 1
      remain = remain - 1
   end
   while num > 0 do
      table.insert(mynames, names[i])
      i = i + 1
      num = num - 1
   end
   -- Send the request to other daemon
   msg["local"] = true
   for id, conn in context.dmn_iter() do
      num = each
      if remain > 0 then
         num = each + 1
         remain = remain - 1
      end
      if num > 0 then
         msg.names = { }
         while num > 0 do
            table.insert(msg.names, names[i])
            i = i + 1
            num = num - 1
         end
         alua.event.send(conn, "spawn", msg, cb)
      else
         break
      end
   end
   msg.names = mynames
   dospawn(msg, cb)
end

--
-- Receive a request to spawn processes in specified daemons
-- by names or number. This function handles the request and
-- submit new requests for the appropriate daemons.
--
local function spawnbydaemon(msg, reply)
   local nresp = 0
   for k, v in pairs(msg.daemons) do
      if (k ~= alua.id and not context.dmn_getconn(k)) or
         (type(v) ~= "number" and type(v) ~= "table")  then
         reply({
            status = "error",
            error = string.format("unknown daemon '%s'", k),
         })
         return
      end
      nresp = nresp + 1
   end
   -- Callback to receive the daemons reply
   local resp = { }
   local cb = function(m)
      if m.status == 'ok' then
         for k, v in pairs(m.processes) do
            resp[k] = v
         end
      end
      nresp = nresp - 1
      if nresp == 0 then
         -- Not empty
         if next(resp) then  
            reply({status = "ok", processes = resp})
         else
            reply({status = "error", error = "cannot spawn the processes"})
         end
      end
   end
   -- Send the requests
   local req = {["local"] = true}
   for k, v in pairs(msg.daemons) do
      if k ~= alua.id then
         if type(v) == "number" then
            req.count = v
            req.names = nil
         else
            req.count = nil
            req.names = v
         end
         alua.event.send(context.dmn_getconn(k), "spawn", req, cb)
      end
   end
   -- Local spawn
   local v = msg.daemons[alua.id]
   if v then
      local t = type(v)
      if t == "number" then
         req.count = v
         req.names = nil
      else
         req.count = nil
         req.names = v
      end
      dospawn(req, cb)
   end
end

--
-- This is the interface that receives all requests and
-- chooses the appropriate spawn method.
--
function evt_spawn(msg, reply)
   if msg["local"] then
      dospawn(msg, reply)
   elseif msg.count then
      spawnbynumber(msg, reply)
   elseif msg.daemons then
      spawnbydaemon(msg, reply)
   elseif msg.names then
      spawnbyname(msg, reply)
   else
      reply({status = "error", error = "invalid request"})
   end
end
