-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's licence can be found 
-- in the LICENCE file.
--

module("alua.channel.mailbox", package.seeall)

-- The mailboxes
local boxes = {}
-- Mailbox owners
local owner = {}
-- The write only connections for the mailboxes.
local writers = {}

-- Save the closed connection in order to fire the 'close' event.
local closed = {}

-- Save the handlers
local handlers = {
   read  = {},
   write = {},
   close = {},
}
-- Set all handlers as weak keys in order to collect the invalid connections.
local weak = { __mode = "k" }
for name, tb in pairs(handlers) do
   setmetatable(tb, weak)
end

-- Define the metatables, they will be fill below.
local metaowner  = { }
local metawriter = { }
local metaclosed = { }


--
-- Default closed operation.
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
-- Send message throght the connection.
--
local function send(conn, msg)
   if not msg then
      return nil, "invalid value"
   end
   local mail = boxes[conn.name]
   if mail then
      table.insert(mail, msg)
      return true
   end
   return nil, "mailbox not found"
end

--
-- Receive a message from the connection.
--
local function receive(conn)
   local mail = boxes[conn.name]
   if mail then
      local msg = mail[1]
      table.remove(mail, 1)
      return msg
   end
   return nil, "mailbox not found"
end

--
-- Close an outgoing channel.
--
local function wrclose(conn)
   closed[conn] = true
   writers[conn.name][conn] = nil
   setmetatable(conn, metaclosed)
end

--
-- Close the mailbox and invalidate all writers.
--
local function close(conn)
   closed[conn] = true
   for wr in pairs(writers[conn.name]) do
      closed[wr] = true
      setmetatable(wr, metaclosed)
   end

   boxes[conn.name] = nil
   owner[conn.name] = nil
   writers[conn.name] = nil
   setmetatable(conn, metaclosed)
end


--
-- Create a new mailbox.
--
function create(type, config, hdls)
   if owner[config.name] then
      return nil, "mailbox already exists"
   end

   local conn = { 
      name = config.name,
      type = "mailbox",
   }
   setmetatable(conn, metaowner)

   boxes[conn.name] = {}
   owner[conn.name] = conn
   writers[conn.name] = {}

   -- Set the handlers
   if hdls then
      handlers.read[conn] = hdls.read
      handlers.write[conn] = hdls.write
      handlers.close[conn] = hdls.close
   end

   return conn
end

--
-- Create a outgoing mailbox connection.
--
function outgoing(type, config, hdls)
   if not owner[config.name] then
      return nil, "mailbox not found"
   end

   local conn = { 
      name = config.name,
      type = "mailbox:out",
   }
   setmetatable(conn, metawriter)
   writers[conn.name][conn] = true

   -- Set the handlers
   if hdls then
      handlers.write[conn] = hdls.write
      handlers.close[conn] = hdls.close
   end

   return conn
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

   -- 'close' event
   clactive = closed
   closed = {}
   -- 'read' event
   for name, mail in pairs(boxes) do
      if #mail > 0 then
         local conn = owner[name]
         rdactive[conn] = true
      end
   end
   -- 'write' event
   for conn, hdl in pairs(handlers.write) do
      wractive[conn] = true
   end

   -- Fire the events

   -- 'read' event
   for conn in pairs(rdactive) do
      -- The connection may be closed
      local hdl = handlers.read[conn]
      if hdl and getmetatable(conn) ~= metaclosed then
         hdl(conn)
         count = count + 1
      end
   end
   -- 'write' event
   for conn in pairs(wractive) do
      -- The connection may be closed
      local hdl = handlers.write[conn]
      if hdl and getmetatable(conn) ~= metaclosed then
         hdl(conn)
         count = count + 1
      end
   end

   -- 'close' event
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
   return "Mailbox channel (closed)"
end

metaowner.__index = {
   send = send,
   receive = receive,
   close = close,
   gethandler = gethandler,
   sethandler = sethandler,
}
metaowner.__tostring = function() 
   return "Mailbox channel" 
end

metawriter.__index = {
   send = send,
   close = wrclose,
   gethandler = gethandler,
   sethandler = sethandler,
}
metawriter.__tostring = function() 
   return "Mailbox channel (write only)"
end
