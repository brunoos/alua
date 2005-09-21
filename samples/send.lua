-- public domain
alua = require("alua")

local procs, buf, app = {}, [[ print(alua.id .. " says hi!") ]], "my app"

function send2_callback(reply)
	print("reply table is " .. _alua.utils.dump(reply))
	alua.exit(procs); alua.exit()
end

function send1_callback(reply)
	print("reply table is " .. _alua.utils.dump(reply))
	print("sending message to all processes at once...")
	alua.send(procs, buf, send2_callback)
end

function
spawn_callback(reply)
	print("sending message process by process...")
	for id, proc in reply.processes do
		alua.send(id, buf, send1_callback); table.insert(procs, id)
	end
end

function start_callback(reply)
	alua.spawn(app, 12, spawn_callback)
end

alua.open()
alua.start(app, start_callback)
alua.loop()
