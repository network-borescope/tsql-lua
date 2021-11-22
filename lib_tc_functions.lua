local _m = {}

-------------------------------------
--
--
--
--
function _m.buildError(err)
    return string.format('{ "tp":0, "id":0, "err": "%s"}', err)
end


-------------------------------------
--
--
--
--
function _m.str_to_epoch(s)
    local year, month, day, hour, min, sec
    if not s:find(" ") then
        -- date only
        year, month, day = s:match("(%d+)-(%d+)-(%d+)");  hour, min, sec = 0, 0, 0
    else
        -- date time
        year, month, day, hour, min, sec = s:match("(%d+)-(%d+)-(%d+) (%d+):(%d+):(%d+)")
    end
    if (not year) or (not month) or (not day) then error("Invalid date: "..s) end
    local offset = os.time() - os.time(os.date("!*t"))
    local tm = os.time({day=day,month=month,year=year,hour=hour,min=min,sec=sec}) + offset
    -- print(s, tm, os.date("!%c",tm))
    return tm
end

return _m