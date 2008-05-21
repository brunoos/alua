-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's license can be found 
-- in the LICENSE file.
--

module("alua.channel.tcp", package.seeall)

-- Auxiliaries modules
require("socket") 
require("alua.config")
require("alua.util.khash")


-- Define the metatables, they will be fill below.
local metaclosed = { }
local metaserver = { }
local metaclient = { }

-- Save the closed connection in order to fire the 'close' event.
local closed = { }

-- Save the handlers.
local handlers = {
   read   = { },
   write  = { },
   close  = { },
   accept = { },
}

-- Save the sockets and connections in order to use them in socket.select().
local listen = {
   read  = alua.util.khash.create(),
   write = alua.util.khash.create(),
}

-- Save the socket connections
local socks = { }

-- Set all handlers as weak keys in order to collect the invalid connections.
local kweak = {__mode = "k"}
for name, tb in pairs(handlers) do
   setmetatable(tb, kweak)
end

--
-- Null operations.
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
   -- Update information to socket.select()
   if getmetatable(conn) ~= metaclosed then
      if hdl then
         if name == "write" then
            listen.write:put(socks[conn], conn)
         else
            -- Handlers: read, close, and accept
            listen.read:put(socks[conn], conn)
         end
      else
         -- The handler is 'nil', remove the socket
         if name == "write" then
            listen.write:remove(socks[conn])
         else
            -- Remove only if there are no handlers
            if not (handlers.read[conn] or
                    handlers.close[conn] or 
                    handlers.accept[conn]) 
            then
               listen.read:remove(socks[conn])
            end
         end
      end         
   end
end

--
-- Close a channel.
--
local function close(conn)
   local sock = socks[conn]
   -- Signalize the 'close' event
   closed[conn] = true
   -- Remove socket.select() information
   listen.read:remove(sock)
   listen.write:remove(sock)
   -- Clean up
   sock:close()
   socks[conn] = nil
   setmetatable(conn, metaclosed)
end

--
-- Send the data.
--
local function send(conn, data)
   return socks[conn]:send(data)
end

--
-- Receive bytes from the connection.
--
local function receive(conn, patt)
   return socks[conn]:receive(patt)
end

--
-- Set the socket timeout.
--
local function settimeout(conn, value, mode)
   socks[conn]:settimeout(value, mode)
end

--
-- Configure the TCP connect options.
--
local function settcpoptions(sck)
   if alua.config.tcp and alua.config.tcp.keepalive then
      sck:setoption('keepalive', alua.config.tcp.keepalive)
   end
   if alua.config.tcp and alua.config.tcp.nodelay then
      sck:setoption('tcp-nodelay', alua.config.tcp.nodelay)
   end
end

--
-- Accept the new connection and fire the 'accept' event.
--
local function accept(server)
   local clt, err = socks[server]:accept()
   if not clt then
      return nil, err
   end
   settcpoptions(clt)
   -- Create the connection object and save its context
   local addr, port = clt:getsockname()
   local conn = {
      addr = addr,
      port = port,
      type = "tcp:client",
   }
   socks[conn] = clt
   setmetatable(conn, metaclient)
   return conn
end


--
-- Create a client channel.
--
function client(type, config, hdls)
   local clt, err = socket.connect(config.addr, config.port)
   if not clt then 
      return nil, err
   end
   settcpoptions(clt)
   -- Create the connection object and save its context
   local addr, port = clt:getsockname()
   local conn = {
      addr = addr, 
      port = port,
      type = "tcp:client",
   }
   socks[conn] = clt
   setmetatable(conn, metaclient)
   -- Set the handlers
   if hdls then
      handlers.read[conn] = hdls.read
      handlers.write[conn] = hdls.write
      handlers.close[conn] = hdls.close
      -- Set information to socket.select()
      if hdls.write then
         listen.write:put(socks[conn], conn)
      end
      if hdls.read or hdls.close then
         listen.read:put(socks[conn], conn)
      end
   end

   return conn
end

--
-- Create a server channel.
--
function server(type, config, hdls)
   -- Create a new server
   local srv, err = socket.bind(config.addr, config.port)
   if not srv then 
      return nil, err
   end
   settcpoptions(srv)
   -- Configure the connection object and its functions
   local addr, port = srv:getsockname()
   local conn = {
      addr = addr,
      port = port,
      type = "tcp:server",
   }
   socks[conn] = srv
   setmetatable(conn, metaserver)
   -- Set the handlers
   if hdls then
      handlers.accept[conn] = hdls.accept
      handlers.close[conn] = hdls.close
      -- Set information to socket.select()
      if hdls.accept then
         listen.read:put(socks[conn], conn)
      end
   end
   return conn
end

--
-- Poll the channels, looking for new events.
--
function poll(timeout)
   -- Count the number of events
   local count = 0
   -- Sockets
   local read = listen.read
   local write = listen.write
   local rdsock = read:keys()
   local wrsock = write:keys()
   -- 'read' and 'accept' events
   local rdactive, wractive = socket.select(rdsock, wrsock, timeout)
   for _, s in ipairs(rdactive) do
      -- The connection can be closed or the handler can be removed:
      -- test before to use them.
      local conn = read:get(s)
      if conn then
         local hdl = handlers.read[conn] or handlers.accept[conn]
         if hdl then
            local meta = getmetatable(conn)
            if meta == metaclient then
               hdl(conn)
               count = count + 1
            elseif meta == metaserver then
               local clt = accept(conn)
               if clt then
                  hdl(clt, conn)
                  count = count + 1
               end
            end
         end
      end
   end
   -- 'write' events
   for _, s in ipairs(wractive) do
      -- The connection can be closed or the handler can be removed:
      -- test before to use them.
      local conn = write:get(s)
      if conn then
         local hdl = handlers.write[conn]
         if hdl then
            hdl(conn)
            count = count + 1
         end
      end
   end
   -- 'close' events
   local clactive = closed
   closed = {}
   for conn in pairs(clactive) do
      -- The handler could be removed: test before to use it
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
   settimeout = opclosed,
   closed = true,
}
metaclosed.__tostring = function() 
   return "TCP channel (closed)"
end

metaclient.__index = {
   send = send,
   receive = receive,
   close = close,
   gethandler = gethandler,
   sethandler = sethandler,
   settimeout = settimeout,
}
metaclient.__tostring = function() 
   return "TCP channel (client)" 
end

metaserver.__index = {
   close = close,
   gethandler = gethandler,
   sethandler = sethandler,
   settimeout = settimeout,
}
metaserver.__tostring = function() 
   return "TCP channel (server)" 
end
