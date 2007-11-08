require("alua")

local procs = {}

function spawncb3(reply)
   print("\n-- 2nd spawn answer --")
   if reply.status == "ok" then
      for k, v in pairs(reply.processes) do
         print("-> ", k)
         for a, b in pairs(v) do
            print("",a, b)
         end
      end
   else
      print("Error: ", reply.error)
   end
   print("--\n")
   alua.exit(procs)
   alua.exit()
end

function spawncb2(reply)
   print("\n-- 1st spawn answer --")
   if reply.status == "ok" then
      for k, v in pairs(reply.processes) do
         print("-> ", k)
         for a, b in pairs(v) do
            print("",a, b)
         end
      end
   else
      print("Error: ", reply.error)
   end
   print("--\n")
   print("spawning processes A, B, C and D *again*...")
   alua.spawn({ "A", "B", "C", "D" }, spawncb3)
   for proc in pairs(reply.processes) do 
      table.insert(procs, proc) 
   end
end

function spawncb1(reply)
   print("spawning processes A, B, C and D...")
   alua.spawn({ "A", "B", "C", "D" }, spawncb2)
   for proc in pairs(reply.processes) do 
      table.insert(procs, proc) 
   end
end

function opencb(reply)
   print("spawning 7 new processes...")
   alua.spawn(7, spawncb1)
end
alua.open({addr="127.0.0.1", port=6080}, opencb)
alua.loop()
