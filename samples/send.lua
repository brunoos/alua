require("alua")

local procs, buf, count = {}, "buf = 123", 12
local show = [[print(alua.id, "buffer = " .. tostring(buf))]]

function sendcb2(reply)
   print("Terminating all processes...")
   alua.exit(procs)
   alua.exit()
end

function sendcb1(reply)
   count = count - 1
   if count == 0 then
      print("Sending message to all processes at once...")
      alua.send(procs, show, sendcb2)
   end
end

function spawncb(reply)
   print("Sending message process by process...")
   for id, proc in pairs(reply.processes) do
      alua.send(id, buf, sendcb1)
      table.insert(procs, id) 
   end
end

function opencb(reply)
   alua.spawn(count, spawncb)
end

alua.open({addr="127.0.0.1", port=6080}, opencb)
alua.loop()
