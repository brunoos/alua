-- public domain
alua = require("alua")

local procs, app = {}, "myapp"

function query3_callback(reply)
	print("reply table is: " .. _alua.utils.dump(reply))
	alua.exit(procs); alua.exit()
end

function query2_callback(reply)
	print("reply table is: " .. _alua.utils.dump(reply))
	alua.spawn(app, 5, spawn_callback)
end

function query1_callback(reply)
	print("reply table is: " .. _alua.utils.dump(reply))
	alua.start(app, start_callback)
end

function spawn_callback(reply)
	for proc in reply.processes do table.insert(procs, proc) end
	print("querying for spawned-on application...")
	alua.query(app, query3_callback)
end

function start_callback(reply)
	print("querying for just-started application...")
	alua.query(app, query2_callback)
end

alua.open()
print("querying for non-existing application...")
alua.query(app, query1_callback)
alua.loop()
