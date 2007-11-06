-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's licence can be found 
-- in the LICENCE file.
--

module("alua.daemon", package.seeall)

-- Standard modules
require("table")
require("string")
-- Internal modules
require("alua.config")
require("alua.core")
require("alua.channel")
require("alua.task")
require("alua.event")
require("alua.process")
require("alua.daemon.context")
require("alua.daemon.auth")
require("alua.daemon.link")
require("alua.daemon.spawn")
require("alua.daemon.message")


---------------------------------------------------------------------------
--                  Low-level Events (channel events)
---------------------------------------------------------------------------

-- Channel events
local chn_handlers

--
-- Clean the data about the remote process.
--
local function chn_close(conn)
   local id = context.prc_getid(conn)
   if id then
      context.prc_clean(id, conn)
      -- not a internal name
      if not string.match(id, "^%d+@%d+%.%d+%.%d+%.%d+:%d+$") then
         for i, c in context.dmn_iter() do
            alua.event.send(c, "notify", {type = "leave", process = id})
         end
      end
   else
      id = context.dmn_getid(conn)
      if id then
         context.dmn_clean(id, conn)
      end
   end
end

--
-- Accept a new process connection.
--
local function chn_accept(conn)
   alua.event.listen(conn, context.events.unknown, chn_close)
end

--
-- Channel handlers.
--
chn_handlers = {
   accept = chn_accept,
}


---------------------------------------------------------------------------
--                           High-level events
---------------------------------------------------------------------------

--
-- Authenticate a process.
--
local function evt_auth(msg, reply, conn)
   if msg.mode then
      local func = context.auth[msg.mode] 
      if func then
         func(msg, reply, conn)
      else
         reply({status = "error", error = "unknown mode"})
      end
   else
      reply({status = "error", error = "unknown mode"})
   end
end

--
-- Close the process connection.
--
local function evt_close(msg, _, conn)
   if msg.mode == "process" then
      conn:close()
   end
end

--
-- Discovery event.
--
local function evt_exists(msg, reply)
   if context.prc_getconn(msg.process) then
      reply({status = "ok", exists = true, daemon = alua.id})
   else
      reply({status = "ok", exists = false})
   end
end

--
-- Notification from other daemon about its processes state.
--
local function evt_notify(msg, reply, conn)
   if msg.type == "leave" then
      context.als_clean(msg.process, conn)
      reply({status = "ok"})
   else
      reply({status = "error", error = "invalid notification"})
   end
end


---------------------------------------------------------------------------
--                           Register the events 
---------------------------------------------------------------------------

--
-- Unauthenticated connection events.
--
context.events.unknown = {
   auth = evt_auth,
}

--
-- Client connection events.
--
context.events.process = { 
   close   = evt_close,
   link    = link.evt_link,
   spawn   = spawn.evt_spawn,
   message = message.evt_message,
}

--
-- Daemon connection events.
--
context.events.daemon = {
   close   = evt_close,
   exists  = evt_exists,
   notify  = evt_notify,
   link    = link.evt_link,
   spawn   = spawn.evt_spawn,
   message = message.evt_message,
}


---------------------------------------------------------------------------
--                             Daemon API
---------------------------------------------------------------------------

--
-- Create a new daemon, launching a new OS process.
--
function create(cfg, cb)
   -- Save the configuration
   local addr = cfg.addr
   local port = cfg.port
   -- Create a channel to wait the new daemon to connect to
   local join = alua.channel.create("tcp:server", 
      {addr = "127.0.0.1", port = 0})
   if not join then
      if cb then
         alua.task.schedule(cb, {status = "error", 
            error = "initialization error"})
      end
      return
   end

   -- Receive data from the new daemon and inform the process
   local joinhandler = function(conn, join)
      join:sethandler("close", nil)
      join:close()
      if not conn then
         if cb then
            cb({status = "error", error = "connection error"})
         end
      else
         conn:send(addr .. "\n")
         conn:send(tostring(port) .. "\n")
         local reply = conn:receive("*l")
         local arg = conn:receive("*l")
         conn:close()
         if cb then
            if reply == "ok" then
               cb({status = "ok", daemon = arg})
            else
               cb({status = "error", error = arg})
            end
         end
      end
   end

   -- Connection error, reply this error to the process
   local joinclose = function(conn)
      if cb then
         cb({status = "error", error = "connection closed"})
      end
   end

   -- Set the handlers to responde to the new daemon connection
   join:sethandler("accept", joinhandler)
   join:sethandler("close", joinclose)
   -- Launch the daemon
   local str = string.format("alua.daemon.launch(%d)", join.port)
   alua.core.execute(alua.config.lua, "-l", "alua", "-e", str)
end


---------------------------------------------------------------------------
--                         Launch a New Daemon
---------------------------------------------------------------------------

-- 
-- This function initiate a new port and wait for events.
--
function launch(point)
   local join = alua.channel.create("tcp:client", 
      {addr = "127.0.0.1", port = point})
   if not join then
      --print("[ERRO] Cannot connect")
      return
   end
   -- Read the configuration for the daemon
   local config = {}
   config.addr = join:receive("*l")
   config.port = tonumber(join:receive("*l"))
   -- Try to create the incoming connection for the processes
   local serverconn, err = alua.channel.create("tcp:server", config, 
      chn_handlers)
   if not serverconn then
      join:send("error\n")
      join:send(err .. "\n")
      join:close()
      return
   end

   -- Create the daemon identification based on the created channel
   -- Use the channel configuration because the port or address wildcards
   alua.id = serverconn.addr .. ":" .. tostring(serverconn.port)
   alua.daemonid = alua.id

   -- Reply success initialization
   join:send("ok\n")
   join:send(alua.id .. "\n")
   join:close()

   -- Create a internal connection in order to the process part of the
   -- daemon can communication with itself without network.
   local daemonconn, processconn = alua.channel.create("mempair")
   alua.event.listen(daemonconn, context.events.process)
   alua.process.setconn(processconn)

   -- Save the server configuration
   context.serverconn = serverconn
   context.daemonconn = daemonconn
   context.processconn = processconn

   -- Wait for events
   alua.loop()
end
