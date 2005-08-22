alua = require("alua")

function
join_callback(reply)
  	if reply.status ~= "ok" then
		print("Failed to join application " .. reply.name)
		print("Error: " .. reply.error)
	else
		print("Joined " .. reply.name)
	end
end

function
start_callback(reply)
  	if reply.status ~= "ok" then
		print("Failed to create application " .. reply.name)
		print("Error: " .. reply.error)
	else
		print("Created " .. reply.name)
	end
end

function
query_callback(reply)
  	if reply.processes then
		alua.join(reply.name, join_callback)
	else
		alua.start(reply.name, { "209.241.5.12:5265" }, start_callback)
	end
end

alua.open()
alua.query("application x", query_callback)
alua.loop()
