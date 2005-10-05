-- public domain
alua = require("alua")

local app = "my app"

function
join_callback(reply)
	print("reply table is: " .. _alua.utils.dump(reply))
	alua.exit()
end

alua.open()
alua.join(app, join_callback)
alua.loop()
