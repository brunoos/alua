-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's licence can be found 
-- in the LICENCE file.
--

module("alua.codec.stream", package.seeall)

require("alua.codec")

-- Aliases
local dump = alua.codec.dump
local load = alua.codec.load

--
-- Send a internal message.
--
function sendmsg(conn, msg)
   msg = dump(msg)
   local size = #msg
   -- Use the \newline character as delimiter for socket connection
   conn:send(tostring(size) .. "\n")
   conn:send(msg)
end

----
--
-- Receive a new internal message.
-- This function can return 'nil' as valid value. So, for error testing,
-- check if the error message is present.
--
function receivemsg(conn)
   local err, size
   -- Receive the header
   size, err = conn:receive("*l")
   if err then
      return nil, err
   end
   size = tonumber(size)

   -- Receive the payload
   local buf, msg
   local remain = size
   while remain > 0 do
      buf, err = conn:receive(remain)
      if not buf then
         if err ~= 'timeout' then
            return nil, err
         end
      else
         if msg then
            msg = msg .. buf
         else
            msg = buf
         end
         remain = size - #msg
      end
   end
   -- Unmarshal
   local succ
   succ, msg = load(msg)
   if succ then
      return msg
   end
   return nil, msg
end
