-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's license can be found 
-- in the LICENSE file.
--

module("alua.daemon.auth", package.seeall)

-- Standard modules
require("table")
require("string")
-- Internal modules
require("alua.event")
require("alua.daemon.context")

-- Alias
local context = alua.daemon.context

--
--
--
local function auth_process(msg, reply, conn)
   -- Save the context
   local id = tostring(context.nextidx()) .. "@" .. alua.id
   -- Use the idx in order to guarantee the sequencial number
   context.prc_save(id, conn)
   -- Set the allowed events
   alua.event.flush(conn)
   alua.event.add(conn, context.events.process)
   reply({status = "ok", id = id, daemon = alua.id})
end

--
--
--
local function auth_daemon(msg, reply, conn)
   -- Save the context
   context.dmn_save(msg.daemon, conn)
   -- Set the allowed events
   alua.event.flush(conn)
   alua.event.add(conn, context.events.daemon)
   reply({status = "ok", daemon = alua.id})
end


--
-- Register the authentication mode for processes and daemons
--
context.auth["daemon"] = auth_daemon
context.auth["process"] = auth_process
