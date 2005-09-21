-- public domain
alua = require("alua")

local app = "myapp"

function
exit_callback(reply)
	print("reply table is: " .. _alua.utils.dump(reply))
	alua.exit()
end

function
spawn_callback(reply)
	local procs = {}
 	for proc in reply.processes do table.insert(procs, proc) end
	print("sending exist request...")
	alua.exit(procs, 0, exit_callback)
end

function start_callback(reply)
	alua.spawn(app, 4, spawn_callback)
end

alua.open()
alua.start(app, start_callback)
alua.loop()
