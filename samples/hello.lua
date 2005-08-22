alua = require("alua")

function
spawn_callback(reply)
	local cmd = [[ print("Hello from " .. alua.id .. "!"); alua.exit() ]]
	for p in reply.processes do alua.send(p, cmd) end
end

alua.open()
alua.start("foobar")
alua.spawn("foobar", 7, spawn_callback)
alua.loop()
