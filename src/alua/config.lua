-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's licence can be found 
-- in the LICENCE file.
--

module("alua.config")

-- Set the Lua interpreter name
lua = "lua"

-- Poll timeout
timeout = 0.1

-- Set configuration for the TCP channel
tcp = { 
   nodelay = true,
   keepalive = true,
}