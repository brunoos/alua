alua = require("alua")

function
leave_callback(reply)
	for proc, termination in reply do
		if termination.status == "ok" then
			print("Process " .. proc .. " terminated")
		end
	end
end

function
spawn_callback(reply)
 	for proc, spawn in reply.processes do
		if spawn.status == "ok" then
			alua.exit(proc, leave_callback)
		end
	end
end

alua.open()
alua.start("application x")
alua.spawn("application x", 4, spawn_callback)
alua.loop()
