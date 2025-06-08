math.randomseed(os.time())

request = function()
  local user_id = "user" .. math.random(1, 1000000)
  local item_id = "item" .. math.random(1, 1000000)
  local path = "/checkout?user_id=" .. user_id .. "&id=" .. item_id

  return wrk.format("POST", path)
end
