-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's licence can be found 
-- in the LICENCE file.
--

--
-- Execute a string.
--
-- This function must be defined before the 'module()' call or without it,
-- otherwise the function will not inherit the global environment.
--
local function dostring(str)
   -- Load the string into an executable object
   local obj, err = loadstring(str)
   if not obj then
      return false, err
   end
   -- Execute the string
   return pcall(obj)
end


module("alua.process", package.seeall)

-- Standard modules
require("table")
require("string")
-- Internal modules
require("alua")
require("alua.channel")
require("alua.task")
require("alua.event")

---------------------------------------------------------------------------
--                  Low-level events (channel events)
---------------------------------------------------------------------------

local function chn_close()
   alua.id = nil
   alua.conn = nil
   alua.daemonid = nil
end

local chn_handlers = {
   close = chn_close
}

---------------------------------------------------------------------------
--                           High-level events
---------------------------------------------------------------------------

--
-- Save the current reply function in order to send back the process 
-- termination. 
-- See evt_message() and exit()
--
local currentreply

--
-- Handle the messages recept by the process.
--
local function evt_message(msg, reply)
   currentreply = reply
   local succ, errmsg = dostring(msg.data)
   currentreply = nil
   if not succ then
      reply({status = "error", error = errmsg})
   else
      reply({status = "ok"})
   end
end

local evt_handlers = {
   message = evt_message,
}

---------------------------------------------------------------------------
--                             Internal Functions
---------------------------------------------------------------------------

--
-- The daemon use this function to set the internal connection with itself.
--
function setconn(conn)
   alua.conn = conn
   alua.event.listen(alua.conn, evt_handlers)
end

---------------------------------------------------------------------------
--                             Process API
---------------------------------------------------------------------------

--
-- Terminate the current process. If it's a remote request, send back the
-- confirmation before exit.
--
local function terminate(code)
   if currentreply then
      currentreply({status = "ok"})
   end
   os.exit(code)
end

--
-- Connect to the daemon.
--
function connect(str, cb)
   if alua.id then
      if cb then
         alua.task.schedule(cb, {status = "error", error = "already connected"})
      end
      return
   end

   local addr, port = string.match(str, "^(%d+%.%d+%.%d+%.%d+):(%d+)$")
   if not addr then
      if cb then
         alua.task.schedule(cb, {status = "error", 
            error = "invalid daemon address"})
      end
      return
   end
   port = tonumber(port)

   local conn, err = alua.channel.create("tcp:client", 
      {addr = addr, port = port})
   if conn then
      -- Create an internal callback to receive the daemon reply
      local reply = function(msg)
         -- Save the id received from the daemon ...
         if msg.status == "ok" then
            alua.id = msg.id
            alua.conn = conn
            alua.daemonid = msg.daemon
            -- Set the events
            for name, hdl in pairs(evt_handlers) do
               alua.event.add(alua.conn, name, hdl)
            end
         else
            -- ... or close the connection if the authentication fail
            conn:close()
         end
         if cb then
            alua.task.schedule(cb, msg)
         end
      end

      -- Activate the connection and send the authentication request
      alua.event.listen(conn)
      alua.event.send(conn, "auth", {mode = "process"}, reply)
   else
      if cb then
         alua.task.schedule(cb, {status = "error", error = err})
      end
   end
end

--
-- Close the connection with the daemon.
--
function close()
   if not alua.id then
      return false, "not connected"
   end
   -- Daemon cannot close a connection with itself
   -- Remark: a daemon has the same value to the fields 'id' and 'daemonid'
   if alua.id ~= alua.daemonid then
      alua.event.send(alua.conn, "close", {mode = "client"})
      alua.conn:close()
      alua.id = nil
      alua.conn = nil
      alua.daemonid = nil
      return true
   end
   return false, "not a process"
end

--
-- Send a message to be executed to the daemon.
-- This function always send a table of processes to the daemon.
--
function send(dst, str, cb)
   local newdst
   if not alua.id then
      if cb then
         alua.task.schedule(cb, {status = "error", error = "not connected"})
      end
      return
   elseif dst == alua.id then
      newdst = dst
   elseif type(dst) == "string" then
      newdst = { dst }
   elseif type(dst) == "table" then
      if #dst == 0 then
         if cb then
            alua.task.schedule(cb, {
               status = "error", 
               error = "invalid destination"
            })
         end
         return
      end
      -- Remove duplicate destinations
      local tmp = { }
      newdst = { }
      for k, v in ipairs(dst) do
         if type(v) ~= "string" then
            if cb then
               alua.task.schedule(cb, {
                  status = "error", 
                  error = "invalid destination",
               })
            end
            return
         elseif not tmp[v] then
            tmp[v] = true
            table.insert(newdst, v)
         end
      end
   else
      if cb then
         alua.task.schedule(cb, {
            status = "error", 
            error = "invalid destination",
         })
      end
      return
   end
   local msg = {
      src = alua.id,
      dst = newdst, 
      data = str,
   }
   -- Avoid network communication. 
   if dst == alua.id then
      -- Create a reply function, even though the callback is 'nil'.
      -- The events expect a reply function.
      local reply = function(data)
         if cb then
            cb(data)
         end
      end
      alua.task.schedule(evt_message, msg, reply)
   else
      alua.event.send(alua.conn, "message", msg, cb)
   end
end

--
-- Request the termination of the remote process(es).
--
function exit(dst, code, cb)
   if type(dst) == "nil" or type(dst) == "number" then
      terminate(dst)
   elseif not alua.id then
      if cb then
         alua.task.schedule(cb, {status = "error", error = "not connected"})
      end
      return
   elseif type(code) ~= "nil" and type(code) ~= "number" then
      if cb then
         alua.task.schedule(cb, {status = "error", error = "invalid code"})
      end
      return
   elseif type(dst) == "table" then
      if #dst == 0 then
         if cb then
            alua.task.schedule(cb, {
               status = "error", 
               error = "invalid destination"
            })
         end
         return
      end
      -- Remove duplicate destinations
      local tmp = { }
      local newdst = { }
      for k, v in ipairs(dst) do
         if type(v) ~= "string" then
            if cb then
               alua.task.schedule(cb, {
                  status = "error", 
                  error = "invalid destination",
               })
            end
            return
         elseif not tmp[v] then
            tmp[v] = true
            table.insert(newdst, v)
         end
      end
      dst = newdst
   elseif type(dst) ~= "string" then
      if cb then
         task.schedule(cb, {
            status = "error", 
            error = "invalid destination",
         })
      end
      return
   end
   send(dst, "alua.exit(" .. tostring(code) .. ")", cb)
end

--
-- Extend our network of daemons.
--
function link(daemons, cb)
   local err
   if not alua.id then
      err = "not connected"
   elseif type(daemons) ~= "table" or #daemons == 0 then
      err = "invalid arguments"
   end
   if err then
      if cb then
         alua.task.schedule(cb, {status = "error", error = err})
      end
      return
   end

   local tmp = {}
   local list = {}
   -- Insert our daemon into the list in the first position
   table.insert(list, 1, alua.daemonid)
   tmp[alua.daemonid] = true
   -- Remove repeated identifications
   for k, v in ipairs(daemons) do
      if not tmp[v] then
         -- Verify if it is a valid identification
         local addr, port = string.match(v, "^(%d+%.%d+%.%d+%.%d+):(%d+)$")
         if not addr or not port then
            if cb then
               alua.task.schedule(cb, {
                  status = "error", 
                  error = "invalid daemon identification",
               })
            end
            return
         end
         port = tonumber(port)
         tmp[v] = true
         table.insert(list, v)
      end
   end
      
   -- Link was done, request the next daemon to make the links
   local arg = { daemons = list, next = 1 }
   alua.event.send(alua.conn, "link", arg, cb)
end


--
-- Send a request to spawn processes using an array of names
--
local function spawnbyname(param, cb)
   -- Copy only the numerical indeces
   local tb, req = {}, {}
   for k, v in ipairs(param) do
      if type(v) ~= "string" then
         local err = {status = "error", error = "invalid name"}
         alua.task.schedule(cb, err)
         return
      elseif tb[v] then
         local err = {status = "error", error = "duplicate name"}
         alua.task.schedule(cb, err)
         return
      end
      tb[v] = true
      req[k] = v
   end
   alua.event.send(alua.conn, "spawn", {names = req}, cb)
end

--
-- Send a request to spawn process per daemons
--
local function spawnbydaemon(param, cb)
   local req = {}
   for k, v in pairs(param) do
      if type(k) ~= "string"  or 
         (type(v) ~= "number" and type(v) ~= "table") then
         local err = {status = "error", error = "invalid request"}
         alua.task.schedule(cb, err)
         return
      elseif type(v) == "number" then
         req[k] = v
      else
         -- Copy only the numerical indeces
         req[k] = {}
         local tb = {}
         for i, j in ipairs(v) do
            if type(j) ~= "string" then
               local err = {status = "error", error = "invalid name"}
               alua.task.schedule(cb, err)
               return
            elseif tb[j] then
               local err = {status = "error", error = "duplicate name"}
               alua.task.schedule(cb, err)
               return
            end
            tb[j] = true
            req[k][i] = j
         end
      end
   end
   alua.event.send(alua.conn, "spawn", {daemons = req}, cb)
end

--
-- Spawn a new process
--
function spawn(param, cb)
   if not alua.id then
      alua.task.schedule(cb, {status = "error", error = "not connected"})
      return
   end
   if type(param) == "number" then
      alua.event.send(alua.conn, "spawn", {count = param}, cb)
   elseif type(param) == "table" then
      -- Maybe an array of names
      if param[1] then
         spawnbyname(param, cb)
      else
         spawnbydaemon(param, cb)
      end
   else
      alua.task.schedule(cb, {status = "error", error = "invalid #1 argument"})
   end
end

---------------------------------------------------------------------------
--                             Internal
---------------------------------------------------------------------------

--
-- Process spawned by daemon
--
function launch(cfg)
   local addr, port = string.match(cfg, "^(%d+%.%d+%.%d+%.%d+):(%d+)$")
   port = tonumber(port)
   local conn = alua.channel.create("tcp:client", {addr = addr, port = port})
   if conn then
      -- Create an internal callback to receive the daemon reply
      local reply = function(msg)
         -- Save the id received from the daemon ...
         if msg.status == "ok" then
            alua.id = msg.id
            alua.conn = conn
            alua.daemonid = msg.daemon
            -- Set the events
            alua.event.add(alua.conn, evt_handlers)
         else
            -- ... or close the connection if the authentication fail
            conn:close()
            os.exit(1)
         end
      end
      -- Activate the connection and send the authentication request
      alua.event.listen(conn)
      alua.event.send(conn, "auth", {mode = "spawn"}, reply)
      alua.loop()
   end
end
