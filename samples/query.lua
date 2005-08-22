alua = require("alua")

function
query_callback(reply)
  	if reply.processes then
		print("Application " .. reply.name .. " exists, and has: ")
		for _, proc in reply.processes do print(proc) end
		print("Where " .. reply.master .. " is the master process.")
	else
		print("Application " .. reply.name .. " does not exist.")
	end
end

alua.open()
alua.query("my application", query_callback)
alua.loop()
