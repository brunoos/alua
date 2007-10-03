-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's licence can be found 
-- in the LICENCE file.
--

module("alua.daemon.context", package.seeall)

-- Save the processes and daemons identifications and connections 
local dmn_conn2id = { } -- map connection into daemon id
local dmn_id2conn = { } -- map daemon id into connection
local prc_conn2id = { } -- map connection into process id
local prc_id2conn = { } -- map process id into connection
local als_conn2id = { } -- map connection into alias
local als_id2conn = { } -- map alias into connection
-- Number of known daemons
dmn_count = 0

-- Process identification
local globalcount = 1

-- Create events categories used according the connection identitfication
--   * daemon: events for daemon connections
--   * process: events for client connections
--   * unknown: events for unauthenticated connections
--
events = { }

-- Create a registry for authentication methods
auth = { }

---------------------------------------------------------------------------
--                  Auxiliaries Functions
---------------------------------------------------------------------------

--
-- Daemon funcitons
--

-- Save the remote daemon id and its connection
function dmn_save(id, conn)
   dmn_id2conn[id] = conn
   dmn_conn2id[conn] = id
   dmn_count = dmn_count + 1
end

-- Remove remote daemon the id and connection 
function dmn_clean(id, conn)
   dmn_id2conn[id] = nil
   dmn_conn2id[conn] = nil
   dmn_count = dmn_count - 1
end

-- Retrieve the remote daemon id
function dmn_getid(conn)
   return dmn_conn2id[conn]
end

-- Retrieve the remote daemon
function dmn_getconn(id)
   -- XXX: should we add this daemon into the table dmn_xxx ?
   if id == alua.id then
      return daemonconn
   end
   return dmn_id2conn[id]
end

-- Create an iterator
function dmn_iter()
   return pairs(dmn_id2conn)
end


--
-- Process functions
--

-- Save the id and connection of a process
function prc_save(id, conn)
   prc_id2conn[id] = conn
   prc_conn2id[conn] = id
end

-- Remove the id and connection of a process
function prc_clean(id, conn)
   prc_id2conn[id] = nil
   prc_conn2id[conn] = nil
end

-- Retrieve the id of a process
function prc_getid(conn)
   return prc_conn2id[conn]
end

-- Retrieve connection to a process
function prc_getconn(id)
   return prc_id2conn[id]
end

-- Create an iterator
function prc_iter()
   return pairs(prc_id2conn)
end


--
-- Alias functions
--

-- Save the alias and connection
function als_save(id, conn)
   als_id2conn[id] = conn
   if not als_conn2id[conn] then
      als_conn2id[conn] = { }
   end
   als_conn2id[conn][id] = true
end

-- Retrieve the alias connection
function als_getconn(id)
   return als_id2conn[id]
end

-- Retrieve the list of aliases
function als_getids(conn)
   return als_conn2id[conn]
end

-- Remove the alias
function als_clean(id, conn)
   als_id2conn[id] = nil
   if als_conn2id[conn] then
      als_conn2id[conn][id] = nil
      -- Empty list
      if not next(als_conn2id[conn]) then
         als_conn2id[conn] = nil
      end
   end
end


--
-- Other functions
--

-- Calculate the next index for a new process
function nextidx()
   local tmp = globalcount
   globalcount = globalcount + 1
   return tmp
end
