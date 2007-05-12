-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's licence can be found 
-- in the LICENCE file.
--

-- Miscellanea. Stuff that really doesn't belong anywhere else.
module("_alua.utils", package.seeall)

-- Auxiliary function used to dump a Lua object.
function dump(obj)
   if type(obj) == "table" then -- Recursively dump tables
      local i, v = next(obj)
      if not i then
         return "{}" 
      end
      local buf = "{ "
      while i do
         -- If the element index is a string, print it.
         if type(i) == "string" then
            buf = buf .. '["' .. i .. '"] = '
         end
         buf = buf .. dump(v)
         i, v = next(obj, i)
         -- If there's a next object, comma-separate it.
         if i then 
            buf = buf .. ", "
         end
      end
      return buf .. " }"
   end
   if type(obj) == "string" then
      return string.format("%q", obj)
   end
   -- Numbers, booleans, userdata (unlikely), etc
   return tostring(obj)
end

-- Code for isolating access to nil fields in a table.
function protect(t, f)
   setmetatable(t, { __index = function(t, k)
                                  return rawget(t, k) or f
                               end })
end

-- Generic function for revoking a command
function invalid_command(sock, context, arguments, reply)
   reply({ status = "error", error = "invalid command" })
end
