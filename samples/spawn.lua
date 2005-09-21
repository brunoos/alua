-- public domain
alua = require("alua")

local procs, app = {}, "my app"

function spawn3_callback(reply)
	print("reply table is: " .. _alua.utils.dump(reply))
	alua.exit(procs); alua.exit()
end

function spawn2_callback(reply)
	print("reply table is: " .. _alua.utils.dump(reply))
	print("spawning processes A, B, C and D *again*...")
	alua.spawn(app, { "A", "B", "C", "D" }, spawn3_callback)
	for proc in reply.processes do table.insert(procs, proc) end
end

function spawn1_callback(reply)
	print("reply table is: " .. _alua.utils.dump(reply))
	print("spawning processes A, B, C and D...")
	alua.spawn(app, { "A", "B", "C", "D" }, spawn2_callback)
	for proc in reply.processes do table.insert(procs, proc) end
end

function start_callback(reply)
	print("spawning 7 new processes...")
	alua.spawn(app, 7, spawn1_callback)
end

alua.open()
alua.start(app, start_callback)
alua.loop()
