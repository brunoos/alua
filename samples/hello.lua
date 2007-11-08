require("alua")

function spawncb(reply)
   local cmd = [[ print("Hello from " .. alua.id .. "!"); alua.exit() ]]
   for p in pairs(reply.processes) do
      alua.send(p, cmd)
   end
end

function opencb(reply)
   alua.spawn(7, spawncb)
end

alua.open({addr="127.0.0.1", port=6080}, opencb)
alua.loop()
