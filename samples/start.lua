alua = require("alua")

daemons = { "213.41.150.72:6722", "65.94.34.3:3015", "195.56.229.197:1152",
	    "62.178.75.222:8919", "81.130.6.2:7834", "202.92.229.140:4930" }

function
start_callback(reply)
  	if reply.status ~= "ok" then
		print("Could not start application: " .. reply.error)
	else
		for _, id in daemons do
			if reply.daemons[id].status ~= "ok" then
				print("Could not start application on " .. id)
				print("Error: " .. reply.daemons[id].error)
			end
		end
	end
end

alua.open()
alua.start("jose's application", daemons, start_callback)
alua.start("jose's application", daemons, start_callback)
alua.loop()
