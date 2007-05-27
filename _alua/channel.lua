-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's licence can be found 
-- in the LICENCE file.
--

module("_alua.channel", package.seeall)

require("socket") 
require("_alua.event") 

-- Create a client channel
local function handler(sock, read, write, close)
   local read_callback, write_callback
   if read then
      read_callback = function (sock, context)
         local pattern = context.pattern
         local data = sock:receive(pattern)
         if not data then 
            return _alua.event.del(sock) 
         end
         read(sock, data)
      end
   end
   if write then
      write_callback = function (sock, context) 
         write(sock) 
      end
   end
   _alua.event.add(sock, { read = read_callback, write = write_callback },
                   { terminator = close })
end


-- Create a client channel
function client(host, port, read, write, close)
   s, e = socket.connect(host, port)
   if not s then 
      return nil, e 
   end
   s:setoption('tcp-nodelay', true)
   handler(s, read, write, close)
   return s
end

-- Create a server channel
function server(port, read, write, conn, close)
   local svr, e = socket.bind("*", port)
   if not svr then 
      return nil, e 
   end
   svr:setoption('tcp-nodelay', true)
   -- Prepare special callback
   local callback = function (sock, context) 
                       local s = sock:accept()
                       conn(s)
                       -- New 'child' channel, handle it
                       if s then 
                          handler(s, read, write, close)
                       end
                    end
   _alua.event.add(svr, { read = callback }, { terminator = close })
   return svr
end

-- Close a channel
function close(sock)
   -- Just remove the event
   _alua.event.del(sock) 
end
 
-- Get the pattern being used for a given channel
function getpattern(sock)
   local _, context = _alua.event.get(sock)
   return context.pattern
end

-- Set the pattern to be used for a given channel
function setpattern(sock, pattern)
   local _, context = _alua.event.get(sock)
   context.pattern = pattern
end
