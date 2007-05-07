-- public domain
alua = require("alua")

local procs = {}

function spawn3_callback(reply)
	print("reply table is: " .. _alua.utils.dump(reply))
	alua.exit(procs); alua.exit()
end

function spawn2_callback(reply)
	print("reply table is: " .. _alua.utils.dump(reply))
	print("spawning processes A, B, C and D *again*...")
	alua.spawn({ "A", "B", "C", "D" }, spawn3_callback)
	for proc in pairs(reply.processes) do table.insert(procs, proc) end
end

function spawn1_callback(reply)
	print("reply table is: " .. _alua.utils.dump(reply))
	print("spawning processes A, B, C and D...")
	alua.spawn({ "A", "B", "C", "D" }, spawn2_callback)
	for proc in pairs(reply.processes) do table.insert(procs, proc) end
end

alua.open()
print("spawning 7 new processes...")
alua.spawn(7, spawn1_callback)
alua.loop()
