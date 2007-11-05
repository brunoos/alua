-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's licence can be found 
-- in the LICENCE file.
--

module("alua.event", package.seeall)

-- Standard modules
require("string")
-- Internal modules
require("alua.task")
require("alua.codec")

-- Alias
local codec = alua.codec

-- Connections state
local states = { }

---------------------------------------------------------------------------
--                           Event Control API
---------------------------------------------------------------------------

--
-- Add a new event handler.
--
function add(conn, evt, hdl)
   local state = states[conn]
   if state then
      if type(evt) == "string" and hdl then
         state.handlers[evt] = hdl
      elseif type(evt) == "table" then
         for k, v in pairs(evt) do
            state.handlers[k] = v
         end
      end
   end
end

--
-- Remove the event.
--
function remove(conn, evt)
   local state = states[conn]
   if state then
      state.handlers[evt] = nil
   end
end

--
-- Remove all event handlers.
--
function flush(conn)
   local state = states[conn]
   if state then
      state.handlers = { }
   end
end


---------------------------------------------------------------------------
--                         Internal Functions
---------------------------------------------------------------------------

--
-- Send a internal message.
--
local function sendmsg(conn, header, data)
   if not conn.closed then
      local cdc = codec.getcodec(conn.type)
      cdc.sendmsg(conn, {header = header, data = data})
   end
end

--
-- Handle an incoming event request: find a handler for it and prepare a 
-- reply function to be used.
--
local function request(conn, header, data)
   -- Prepare the reply function for the handler
   local id = header.id
   local replied = false
   local reply = function(data)
      assert(not replied, "reply already sent")
      replied = true
      sendmsg(conn, {type = "reply", id = id}, data)
   end

   -- Invoke the handler
   local handler = states[conn].handlers[header.event]
   if handler then
      handler(data, reply, conn)
   else
      local msg = {status = "error"}
      msg.error = string.format("*** event %q not found", header.event)
      reply(msg)
   end
end

--
-- Handle a reply from a previous request.
--
local function reply(conn, header, data)
   local state = states[conn]
   if state then
      -- Retrieve the callback
      local id = header.id
      local cb = state.callbacks[id]
      -- Invoke the callback, if any
      if cb then
         cb(data)
      end
      -- Clear the pending mark
      state.pending[id] = nil
      state.callbacks[id] = nil
   end
end

--
-- Receive a new internal message ('request' or 'reply') and dispatch it.
--
local function receivemsg(conn)
   local state = states[conn]
   if state and not conn.closed then
      local cdc = codec.getcodec(conn.type)
      local msg, err = cdc.receivemsg(conn)
      if err then 
         --print("[DEBUG] " .. err)
         conn:close()
      else
         local header, data = msg.header, msg.data
         if header.type == "request" then
            request(conn, header, data)
         elseif header.type == "reply" then
            reply(conn, header, data)
         end
      end
   end
end

--
-- Wait for the connection close and dispose it.
-- This function propagate the 'close' event.
--
local function cleanup(conn)
   local close = states[conn].close
   local tb = {status = "error", error = "connection closed"}
   for idx, cb in pairs(states[conn].callbacks) do
      cb(tb)
   end
   dispose(conn)
   if close then
      close(conn)
   end
end


---------------------------------------------------------------------------
--                         Communication Functions
---------------------------------------------------------------------------

--
-- Issue an event.
--
function send(conn, evt, data, cb)
   local state = states[conn]
   if not state then
      alua.task.schedule(cb, {
         status = "error", 
         error = "connection not registered",
      })
      return
   end

   if conn.closed then
      alua.task.schedule(cb, {status = "error", error = "connection closed"})
      return
   end

   -- Mark the request as pending and set the callback
   local id = #state.pending + 1
   state.pending[id] = true
   state.callbacks[id] = cb

   -- Send the request
   sendmsg(conn, {type = "request", event = evt, id = id}, data)
end


---------------------------------------------------------------------------
--                           Register Functions
---------------------------------------------------------------------------

--
-- Configure the 'close' handler.
--
function setclose(conn, hdl)
   local state = states[conn]
   if state then
      state.close = hdl
   end
end

--
-- Stop listening on the connection.
--
function dispose(conn)
   if states[conn] then
      conn:sethandler("read", nil)
      conn:sethandler("close", nil)
      states[conn] = nil
   end
end

--
-- Listening on the connection for events.
-- This module uses the 'close' event to clean up the connection state.
-- However, this event can be received in the function 'close' passed as
-- parameter.
--
function listen(conn, evts, close)
   -- Configure the connection state
   states[conn] = {
      pending = {},
      callbacks = {},
      handlers = {},
      close = close,
   }
   -- Listen on this connection
   conn:sethandler("read", receivemsg)
   -- Set the clean up function
   conn:sethandler("close", cleanup)
   -- Set the events
   if evts then
      add(conn, evts)
   end
end
