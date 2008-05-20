-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's license can be found 
-- in the LICENSE file.
--

module("alua.task", package.seeall)

require("alua.channel")

-- Mailbox for scheduled messages (created below)
local tasks

local function dispatch()
   local t = tasks:receive()
   while t do
      t.func(unpack(t.args))
      t = tasks:receive()
   end
end

--
-- Select the codec based on the connection type.
--
function schedule(f, ...)
   if f then
      tasks:send({func = f, args = {...}})
   end
end

-- Create the mailbox
tasks = alua.channel.create("mailbox", {name = "task:mailbox"}, 
   {read = dispatch})
assert(tasks, "cannot create task queue")
