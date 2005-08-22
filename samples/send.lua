alua = require("alua")

function
send_callback(reply)
  	for id, msg in reply do
		if msg.status ~= "ok" then
			print("Process " .. id .. " could not receive message")
			print("Error: " .. msg.error)
		else
			print("Message sent to process " .. id)
		end
	end
end

function
spawn_callback(reply)
  	for id, proc in reply.processes do
		if proc.status == "ok" then
			alua.send(id, [[ print("Hello World!") ]], send_callback)
		end
	end
end

alua.open()
alua.start("application y")
alua.spawn("application y", 12, spawn_callback)
alua.loop()
