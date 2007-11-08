require("alua")

function exitcb(reply)
   for k, v in pairs(reply) do 
      print("-> " .. k)
      for i, j in pairs(v) do
         print("   " .. i .. " = " .. j)
      end
   end
   alua.exit()
end

function spawncb(reply)
   local procs = {}
   for proc in pairs(reply.processes) do
      table.insert(procs, proc)
   end
   print("Sending exist request...")
   alua.exit(procs, 0, exitcb)
end

function opencb(reply)
   alua.spawn(4, spawncb)
end

alua.open({addr="127.0.0.1", port=6080}, opencb)
alua.loop()
