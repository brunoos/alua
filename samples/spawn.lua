alua = require("alua")

function
spawn_callback(reply)
  	print("Spawn on " .. reply.name)
	for id, proc in reply.processes do
		if proc.status ~= "ok" then
			print("Failed to spawn " .. id .. ": " .. proc.error)
		else
			print(id .. " successfully spawned on " .. proc.daemon)
		end
	end
end

alua.open()
alua.start("new application")
alua.spawn("new application", 7, spawn_callback)
alua.spawn("new application", { "A", "B", "C", "D" }, spawn_callback)
alua.spawn("new application", { "A", "B", "C", "D" }, spawn_callback)
alua.loop()
