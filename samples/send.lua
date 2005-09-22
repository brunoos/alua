-- public domain
alua = require("alua")

local procs, buf, app, count = {}, [[ a = 1 ]], "my app", 12

function send2_callback(reply)
	print("reply table is " .. _alua.utils.dump(reply))
	alua.exit(procs); alua.exit()
end

function send1_callback(reply)
	print("reply table is " .. _alua.utils.dump(reply))
	count = count -1; if count == 0 then
		print("sending message to all processes at once...")
		alua.send(procs, buf, send2_callback) end
end

function
spawn_callback(reply)
	print("sending message process by process...")
	for id, proc in reply.processes do
		alua.send(id, buf, send1_callback); table.insert(procs, id) end
end

function start_callback(reply)
	alua.spawn(app, count, spawn_callback)
end

alua.open()
alua.start(app, start_callback)
alua.loop()
