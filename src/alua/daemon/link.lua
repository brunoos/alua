-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's license can be found 
-- in the LICENSE file.
--

module("alua.daemon.link", package.seeall)

-- Standard modules
require("table")
require("string")
-- Internal modules
require("alua.event")
require("alua.daemon.context")

-- Alias
local context = alua.daemon.context

--
-- Link the daemons.
--
function evt_link(msg, reply, conn)
   -- Process only unknown daemons.
   local unknown = { }
   for k, v in ipairs(msg.daemons) do
      if v ~= alua.id and not context.dmn_getconn(v) then
         table.insert(unknown, v)
      end
   end

   -- If we know all daemons we can forward the request 
   -- (if we are not the last in the list)
   if #unknown == 0 then
      msg.next = msg.next + 1
      if msg.next <= #msg.daemons then
         local conn = context.dmn_getconn(msg.daemons[msg.next])
         alua.event.send(conn, "link", msg, reply)
      else
         reply({ status = "ok", daemons = msg.daemons})
      end
      return
   end

   -- State for deal with the replies
   local errmsg
   local succ = true
   local count = 0
   local connections = { }

   -- Control the link connection reply from the other daemons.
   -- This function is used in the authentication phase below.
   local cb = function(m)
      if m.status == "error" then
         succ = false
         errmsg = errmsg or m.error
      else
         -- Save the context
         local conn = connections[m.daemon]
         context.dmn_save(m.daemon, conn)
         -- Set the allowed events and the clean up function
         alua.event.add(conn, context.events.daemon)
         alua.event.setclose(conn, chn_close)
      end

      -- Wait for the reply of all daemons
      count = count - 1
      if count == 0 then
         if succ then
            -- Forward the request if we are not the last daemon in the list,
            -- or give the reply with a list of daemons.
            msg.next = msg.next + 1
            if msg.next <= #msg.daemons then
               local conn = context.dmn_getconn(msg.daemons[msg.next])
               alua.event.send(conn, "link", msg, reply)
            else
               reply({status = "ok", daemons = msg.daemons})
            end
         else
            reply({ status = "error", error  = errmsg})
         end
      end
   end

   -- Open connection with the daemons and use the two function above to control
   -- the replies from the daemons.
   for k, v in ipairs(unknown) do
      local addr, port = string.match(v, "^(%d+%.%d+%.%d+%.%d+):(%d+)$")
      port = tonumber(port)
      local conn = alua.channel.create("tcp:client", 
         {addr = addr, port = port})
      if not conn then
         succ = false
         errmsg = string.format("[%s] could not connect to %q", alua.id, v)
         break
      else
         -- Send the authentication request
         connections[v] = conn
         alua.event.listen(conn)
         alua.event.send(conn, "auth", {mode = "daemon", daemon = alua.id}, cb)
         count = count + 1
      end
   end
   -- No connection was opened
   if count == 0 then
      reply({status = "error", error = errmsg})
   end
end
