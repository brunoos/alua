-- $Id$
--
-- All rights reserved. Part of the ALua project.
-- Detailed information regarding ALua's license can be found 
-- in the LICENSE file.
--

--
-- This module creates a hashtable optimized to access the keys as array.
--

module("alua.util.khash", package.seeall)

local function put(self, key, value)
   local idx = self._indeces[key]
   if not idx then
      idx = #self._keyserie + 1
      self._keyserie[idx] = key
      self._indeces[key] = idx
   end
   self._values[key] = value
end

local function remove(self, key)
   local idx = self._indeces[key]
   if idx then
      local val = self._values[key]

      self._indeces[key] = nil
      self._values[key] = nil

      local lastidx = #self._keyserie
      if lastidx == idx then
         self._keyserie[idx] = nil
      else
         local lastkey = self._keyserie[lastidx]
         self._keyserie[idx] = lastkey
         self._keyserie[lastidx] = nil
         self._indeces[lastkey] = idx
      end
      return key, val
   end
end

local function get(self, key)
   return self._values[key]
end

local function exists(self, key)
   return (self._indeces[key] ~= nil)
end

local function keys(self)
   return self._keyserie
end

local function _pairs(self)
   return pairs(self._values)
end

local function _ipairs(self)
   return ipairs(self._values)
end

local function size(self)
   return #self._keyserie
end

local htmeta = {
   __index = {
      put = put,
      get = get,
      remove = remove,
      keys = keys,
      pairs = _pairs,
      ipairs = _ipairs,
      exists = exists,
      size = size,
   }
}

function create()
   local ht = {
      _values = {},
      _indeces = {},
      _keyserie = {},
   }
   setmetatable(ht, htmeta)
   return ht
end
