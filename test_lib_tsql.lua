---------------------------------------------------------
-- Todo:
--    Validate the structure 
--------------------------------------------------------

local use_table = {
    ufes = {
        items = {
            options = { pop_id = {map_cat = { AC=1, AM=2, AP=3, BA=4 } }, interface_id=1, location=1 },
            selectors = { time=1 },
            t_measures = { counter=1, ['*']=1 },
            s_measures = { counter=1, ['*']=1, pkt_loss_norm_avg=1, rtt_ns_avg=1, download_kbps_avg=1, upload_kbps_avg=1 },
        }
    },
    rnp_ttls = {
        items = {
            options = { cliente=1, ttl=1, location=1 },
            selectors = { time=1 },
            t_measures = { counter=1, ['*']=1, quantidades=1 },
            s_measures = { counter=1, ['*']=1, hsum=1, hc=1 },
        }
    }
}



-----------------------------------------------------------------
--                 Filter Mappings
-----------------------------------------------------------------


-------------------------------------
--
--
--
--
local function map_to_epoch(s)
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


------------------------------------------------------------------

local PMIN, PMAX, PMAP, PHEAD, PTAIL = 1, 2, 3, 4, 5


local option_filters = {
    eq      = { [PMIN]=1, [PHEAD]="i", [PTAIL]="i" },
    zrect   = { [PMIN]=5, [PMAX]=5, [PHEAD]="iffff" },
    zpoly   = { [PMIN]=7, [PHEAD]="i", [PTAIL]="f" },
    between = { [PMIN]=2, [PMAX]=2, [PMAP]=map_to_epoch, [PHEAD]="ii" },
    range   = { [PMIN]=2, [PMAX]=2, [PMAP]=map_to_epoch, [PHEAD]="ii" },
}


---------------------------------------------------------
--
-- Test
--
--------------------------------------------------------
local str0 = [[
    ;;
    # teste
    ;

    ;
]]

local str1 = [[
    use ufes 
    select rtt_ns_avg 
    # comment
    where time between "2020-10-01", "2020-10-02" 
          and location zrect 5, -12.13, 12.1, 121, 33
          and pop_id eq AC
          and interface_id eq 1
    group by pop_id
]]

local str2 = [[
    use ufes 
    select rtt_ns_avg 
    # comment
    where time between "2020-10-01", "2020-10-02" 
          and location zrect 5, -12.13, 12.1, 121, 33
          and pop_id eq AC
          and interface_id eq 1
    group by pop_id, location
        get avg, min
        length 500;
        ; ; 
        # test to accept empty statements
        ;  ;
    bounds time;
]]

local rnp_ttls = [[
    use rnp_ttls
    select hsum
    where time between "2021-09-01", "2021-09-02" 
    group by time
        using 10 bins
        as avg, min, max
        since 112
        until 1000;
    select hsum
        where time between "2021-09-01", "2021-09-02" 
        group by time
        ;
    

]]

-----------------------------------------------------------------
--                 Exemplos
-----------------------------------------------------------------
--[[

    use ufes
    select ttt_ns_avg
    where pop_id = 10 
       and time range "2021-01-01", "2021-02-01"
    group by time
          get avg, min, max
          length 1000
          bin 2000    # seconds
          start-time ""
          end-time   ""

]]--




local str = rnp_ttls

print(str)

local tsql = require "lib_tsql"

--local skt = require "socket"
--local ms = skt.gettime()*1000
local tm = os.time()
for i = 1, 1 do
    local jsons = {}
    local status, json_str = pcall(tsql.compile_tsql, str, option_filters, use_table)
    if not status then
        print("err:", json_str)
        break
    else
        for k,js in pairs(json_str) do
            if type(js) == "string" then 
                print("jsquery:", js) 
            else
                print("jsquery:", js[1]) 
            end
        end
    end
end
-- print(os.time() - tm)
--print(skt.gettime()*1000 - ms)
