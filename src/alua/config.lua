-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's license can be found 
-- in the LICENSE file.
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
