require("alua")

ncmd = 100
size = 1

function spawncb(reply)
   local cmd = [[ i = 0 while (i < ]] .. size .. [[) do i = i + 1 end ]]
   local i = 1
   while i <= ncmd do
      for proc in pairs(reply.processes) do
         alua.send(proc, string.format("print(%d)", i) .. cmd)
      end
      i = i + 1
   end
   print("The end...")
   alua.exit()
end

function opencb(reply)
   alua.spawn(10, spawncb)
end

alua.open({addr = "127.0.0.1", port=6080}, opencb)
alua.loop()
