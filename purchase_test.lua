-- purchase_test.lua
-- auto-generated; reads codes.txt at runtime
math.randomseed(os.time())

-- load codes
local codes = {}
for line in io.lines("codes.txt") do
  if #line > 0 then
    codes[#codes + 1] = line
  end
end
assert(#codes > 0, "No codes loaded")

request = function()
  local code = codes[math.random(1, #codes)]
  return wrk.format("POST", "/purchase?code=" .. code)
end
