-- public domain
alua = require("alua")

local app = "my app"

function
start2_callback(reply)
	print("reply table is: " .. _alua.utils.dump(reply))
	alua.exit()
end

function
start_callback(reply)
	print("reply table is: " .. _alua.utils.dump(reply))
	alua.start(app, start2_callback)
end

alua.open()
alua.start(app, start_callback)
alua.loop()
