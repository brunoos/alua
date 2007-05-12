-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's licence can be found 
-- in the LICENCE file.
--

module("_alua.event", package.seeall)

-- External modules
require("socket")

local event_panel, read_table, write_table = {}, {}, {}

-- Auxiliar functions for inserting and removing an element from a list

local tmp = {}

local function list_insert(list, element)
   table.insert(list, element)
   tmp[list] = tmp[list] or {}
   tmp[list][element] = #list
end

local function list_remove(list, element)
   local index, last = tmp[list][element], table.remove(list)
   tmp[list][element] = nil
   if last == element then 
      return 
   end
   list[index] = last
   tmp[list][last] = index
end

-- Flush event table
function flush()
   for s in pairs(event_panel) do 
      s:close() 
   end
   read_table, write_table, event_panel  = {}, {}, {}
end

-- Add a new event
function add(sock, callbacks, context)
   if callbacks.read  then 
      list_insert(read_table, sock)
   end
   if callbacks.write then 
      list_insert(write_table, sock) 
   end
   event_panel[sock] = { handlers = callbacks, context = context or {} }
end

-- Delete an event
function del(sock)
   local context = event_panel[sock].context
   if context.terminator then 
      context.terminator(sock, context) 
   end
   local callbacks = event_panel[sock].handlers
   if callbacks.read then 
      list_remove(read_table, sock) 
   end
   if callbacks.write then 
      list_remove(write_table, sock) 
   end
   event_panel[sock] = nil
   sock:close()
end

-- Get the handler and context of a socket
function get(sock)
   return event_panel[sock].handlers, event_panel[sock].context
end

-- Check for activity in the events sockets
function loop()
   local ractive, wactive = socket.select(read_table, write_table, 1)
   -- Do the respective callbacks
   for _, s in ipairs(ractive) do
      local callback = event_panel[s].handlers.read
      if callback then 
         callback(s, event_panel[s].context) 
      end
   end
   -- Do the respective callbacks
   for _, s in ipairs(wactive) do
      local callback = event_panel[s].handlers.write
      if callback then 
         callback(s, event_panel[s].context) 
      end
   end
   -- and return the number of events
   return #read_table + #write_table
end
