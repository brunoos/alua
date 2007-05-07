-- $Id$

-- Distributed matrix multiplication, with minimal code sharing.
-- Written by Pedro Martelletto and Ricardo Costa in March 2005.
-- Public domain.

m1 = {
	{ 44, 68, 27 },
	{ 35, 42, 58 },
	{ 47, 63, 85 },
	{ 74, 67, 66 },
	{ 51, 79, 42 },
}

m2 = {
	{ 24, 45, 60, 10, 44, 53, 44, 17, 13, 23 },
	{ 19, 69, 83, 22, 36, 45, 98, 77, 52, 34 },
	{ 88, 16, 62, 31, 49, 72, 50, 14, 37, 81 },
}

-- Gets row 'i' of matrix 'm'.
function getrow(m, i)
	return m[i]
end

-- Gets column 'i' of matrix 'm'.
function getcol(m, i)
	local col = {}
	for _, v in pairs(m) do table.insert(col, v[i]) end
	return col
end

-- Returns the number of rows in matrix 'm'.
function cntrow(m)
	return table.getn(m)
end

-- Returns the number of columns in matrix 'm'.
function cntcol(m)
	return table.getn(m[1])
end

-- The following chunk will be sent to all processes.
local lincom_buf = [[
-- Returns the linear combination of a (row, column) tuple.
function lincom(r, c)
	local ret = 0
	for i, v in pairs(r) do ret = ret + v*c[i] end
	return ret
end ]]

-- Dumps a matrix on the screen.
function mdump(m)
	for i = 1, cntrow(m) do
		local row = getrow(m, i)
		for _, v in pairs(row) do io.write(v .. ", ") end
		io.write("\n")
	end
end

-- Calculates a row of the resulting matrix, using a given process.
function dorow(idx, p)
	-- Define 'row' in the remote party.
	alua.send(p, "row = " .. alua.tostring(getrow(m1, idx)))
	alua.send(p, "ret = {}")
	for k = 1, cntcol(m2) do
		-- Define 'col' in the remote party.
		alua.send(p, "col = " .. alua.tostring(getcol(m2, k)))
		-- Do the linear combination.
		alua.send(p, "lc = lincom(row, col)")
		alua.send(p, "table.insert(ret, lc)")
	end
	-- Send us back the result.
	alua.send(p, [[ alua.send(alua.parent,
	    string.format("strrow(%s)", alua.tostring(ret))) ]] )
end

-- Stores a row in the resulting matrix.
function strrow(r)
	idx = idx or 1
	res = res or {}
	res[idx] = r
	if idx == cntrow(m1) then
		-- Time to stop.
		mdump(res)
		-- Terminate all processes.
		alua.exit(procs)
		-- And quit.
		alua.exit()
	end
	-- Carry on to the next row.
	idx = idx + 1
	dorow(idx, procs[idx])
end

-- The all-powerful spawn callback.
function spawn_callback(reply)
	procs = {}
	-- Define lincom() in all processes.
	for id in pairs(reply.processes) do
		alua.send(id, lincom_buf)
		table.insert(procs, id)
	end
	-- Begin the calculation.
	dorow(1, procs[1])
end

-- Callback for the link command.
function link_callback(reply)
	-- We spawn cntrow(m1) processes, and use them to
	-- calculate each row of the resulting matrix.
	alua.spawn(cntrow(m1), spawn_callback)
end

daemons = { "127.0.0.1:1234", "127.0.0.1:4321" }
-- The preamble of every ALua application.
alua = require("alua")
alua.create({ port = 1234 })
alua.create({ port = 4321 })
alua.open({ port = 6080 })
-- Link both daemons we are going to use.
alua.link(daemons, link_callback)
alua.loop()
