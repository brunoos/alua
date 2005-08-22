alua = require("alua")

function
leave_callback(reply)
  	if reply.status ~= "ok" then
		print("Could not leave application " .. reply.name)
		print("Error: " .. reply.error)
	else
		print("Ok, left " .. reply.name)
	end
end

alua.open()
alua.join("jose's application")
alua.leave("jose's application", leave_callback)
alua.loop()
