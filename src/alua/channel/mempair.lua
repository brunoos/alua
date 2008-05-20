-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's license can be found 
-- in the LICENSE file.
--

module("alua.channel.mempair", package.seeall)

-- Define the metatables, they will be fill below.
local metapair   = { }
local metaclosed = { }
local metardonly = { }

-- Data buffers
local buffers = {}
-- Map one pair into other
local connpairs = {}

-- Save the closed connection in order to fire the 'close' event.
local closed = {}
-- Notify the closed connections as ready in order to signalize the closed state
local forceready = {}

-- Save the handlers
local handlers = {
   read  = {},
   write = {},
   close = {},
}

-- Used to clean up the state
local kweak  = {__mode = "k"}
local kvweak = {__mode = "kv"}

-- Set as weak keys in order to collect the invalid connections.
for name, tb in pairs(handlers) do
   setmetatable(tb, kweak)
end
setmetatable(buffers, kweak)
setmetatable(forceready, kweak)

-- Set as weak keys *and* values due to mutual reference
setmetatable(connpairs, kvweak)


--
-- Null operation.
--
local function opclosed()
   return nil, "closed"
end

--
-- Get the handler being used for a given channel.
--
local function gethandler(conn, name)
   return handlers[name][conn]
end

--
-- Set the handler to be used for a given channel.
--
local function sethandler(conn, name, hdl)
   handlers[name][conn] = hdl
end

--
-- Send the raw data without reply control.
--
local function send(conn, msg)
   local pair = connpairs[conn]
   if not pairs then
      return nil, "closed"
   end
   if not msg then
      return nil, "invalid message"
   end
   local buf = buffers[pair]
   table.insert(buf, msg)
   return true
end

--
-- Receive the message from the connection.
--
local function receive(conn)
   local buf = buffers[conn]
   local msg = buf[1]
   if msg then
      table.remove(buf, 1)
   elseif not connpairs[conn] then
      return nil, "closed"
   end
   return msg
end

--
-- Close the connections.
--
local function close(conn)
   closed[conn] = true
   local pair = connpairs[conn]
   -- Clear up
   forceready[conn] = nil
   buffers[conn] = nil
   connpairs[conn] = nil
   -- Set the connection as 'closed' and signalize it.
   if pair then
      forceready[pair] = true
      connpairs[pair] = nil
   end
   setmetatable(conn, metaclosed)
end


--
-- Create a new memory pair.
--
function create(type, _, hdls)
   local conn1 = { 
      type = "mempair",
   }
   local conn2 = {
      type = "mempair",
   }
   setmetatable(conn1, metapair)
   setmetatable(conn2, metapair)

   buffers[conn1] = {}
   buffers[conn2] = {}
   connpairs[conn1] = conn2
   connpairs[conn2] = conn1

   -- Set the handlers
   if hdls then
      handlers.read[conn1] = hdls.read
      handlers.write[conn1] = hdls.write
      handlers.close[conn1] = hdls.close
      handlers.read[conn2] = hdls.read
      handlers.write[conn2] = hdls.write
      handlers.close[conn2] = hdls.close
   end

   return conn1, conn2
end


--
-- Poll the channels, looking for new events.
--
function poll()
   local count = 0
   local rdactive = {}
   local wractive = {}
   local clactive

   -- Copy the data in order to avoid inconsistency

   -- close
   clactive = closed
   closed = {}
   -- read
   for conn, buf in pairs(buffers) do
      if #buf > 0 then
         rdactive[conn] = true
      end
      -- Force closed channel to call 'receive' in order to detect the 
      -- the closed state.
      for conn in pairs(forceready) do
         rdactive[conn] = true
      end
   end
   -- write
   for conn in pairs(handlers.write) do
      wractive[conn] = true
   end
   -- Force closed channel to call 'send' in order to detect closed state.
   for conn in pairs(forceready) do
      wractive[conn] = true
   end

   -- Fire the events

   -- read
   for conn in pairs(rdactive) do
      -- The connection may be closed but can have data into the buffer
      local hdl = handlers.read[conn]
      if hdl and getmetatable(conn) ~= metaclosed then
         hdl(conn)
         count = count + 1
      end
   end
   -- write
   for conn in pairs(wractive) do
      -- The connection may be closed
      local hdl = handlers.write[conn]
      if hdl and getmetatable(conn) ~= metaclosed then
         hdl(conn)
         count = count + 1
      end
   end
   -- close
   for conn in pairs(clactive) do
      local hdl = handlers.close[conn]
      if hdl then
         hdl(conn)
         count = count + 1
      end
   end

   return count
end

--
-- Configure the metatables.
--
metaclosed.__index = {
   send = opclosed,
   receive = opclosed,
   close = opclosed,
   gethandler = gethandler,
   sethandler = sethandler,
   closed = true,
}
metaclosed.__tostring = function() 
   return "Memory pair channel (closed)"
end

metapair.__index = {
   send = send,
   receive = receive,
   close = close,
   gethandler = gethandler,
   sethandler = sethandler,
}
metapair.__tostring = function() 
   return "Memory pair channel" 
end
