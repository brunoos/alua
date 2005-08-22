alua = require("alua")

ncmd = 100
size = 1

function
spawn_callback(reply)
	local cmd, i

	cmd = [[ i = 0 while (i < ]] .. size .. [[) do i = i + 1 end ]]

	i = 1
	while i <= ncmd do
		for proc in reply.processes do
			alua.send(proc, string.format("print(%d)", i) .. cmd)
		end
		i = i + 1
	end

	print("The end...")
	alua.exit()
end

alua.open()
alua.start("crash")
alua.spawn("crash", 10, spawn_callback)
alua.loop()
