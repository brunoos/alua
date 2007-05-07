-- public domain
alua = require("alua")

function
exit_callback(reply)
	print("reply table is: " .. _alua.utils.dump(reply))
	alua.exit()
end

function
spawn_callback(reply)
	local procs = {}
 	for proc in pairs(reply.processes) do
		table.insert(procs, proc)
	end
	print("sending exist request...")
	alua.exit(procs, 0, exit_callback)
end

alua.open()
alua.spawn(4, spawn_callback)
alua.loop()
