---------------------------------------------------------
--
--
--------------------------------------------------------



local from_info = {
    ufes = {
        options = { pop_id=1, interface_id=1, location=1 },
        selectors = { time=1 },
        t_measures = { pkt_loss_norm_avg=1, rtt_ns_avg=1, download_kbps_avg=1, upload_kbps_avg=1 },
        s_measures = { pkt_loss_norm_avg=1, rtt_ns_avg=1, download_kbps_avg=1, upload_kbps_avg=1 },

    }
}

local current_from = from_info.ufes

local function str_to_epoch(str)
end


local funs = {
    eq      = { 1, nil },
    zrect   = { 5, 5 },
    zpoly   = { 7, nil },
    between = { 2, 2, str_to_epoch },
    range   = { 2, 2, str_to_epoch },
}

---------------------------------------------------------------
local LITERAL = 1
local STRING = 2

local SELECT = 10
local WHERE = 11
local GROUP_BY = 12
local OUTPUT = 13
local FROM = 14
local BOUNDS = 17
local AND = 15
local COMMA = 16
local GROUP = 18
local BY = 19

---------------------------------------------------------------
local CD=1
local TT=2
local DO=3

---------------------------------------------------------
--
--
--------------------------------------------------------

-------------------------------------
--
--
--
--
local function err_unk_measures(ident)
    local s = ""
    for k, v in pairs(current_from.measures) do
        s = s .. k .. " "
    end
    error("Unknown measure: "..ident..". Valid measures are: { " .. s .. " }")
end

-------------------------------------
--
--
--
--
local function err_unk_where_field(ident)
    local s = ""
    for k, v in pairs(current_from.options) do
        s = s .. k .. " "
    end
    for k, v in pairs(current_from.selectors) do
        s = s .. k .. " "
    end
    error("Unknown where-field: "..ident..". Valid values are: { " .. s .. " }")
end



-------------------------------------
--
--
--
--
local function do_select (q, tokens, pos, n)
    table.insert(q.jstab, '"select": [')
    local t
    while true do
        t = tokens[pos]
        if t[CD] ~= LITERAL then error("Literal expected") end
        local ident = t[TT]
        if not current_from.measures[ident] then err_unk_measures(ident) end
        local s = '"' .. ident .. '"'
        table.insert(q.jstab, s)
        if pos == n then pos = nil; break end
        pos = pos + 1

        t = tokens[pos]
        if t[CD] ~= COMMA then break end
        table.insert(q.jstab, ', ')
    end            
    table.insert(q.jstab, '], ')
    return pos
end
    
-------------------------------------
--
--
--
--
local function do_from (q, tokens, pos, n)
    table.insert(q.jstab, '"from":')
    local t = tokens[pos]
    if t[CD] ~= LITERAL then error("Literal expected") end
    local ident = t[TT]
    if not from_info[ident] then error ('Unknown from: ' .. ident) end
    local s = ' "' .. ident .. '"'
    table.insert(q.jstab, s)
    table.insert(q.jstab, ', ')

    if pos == n then return nil end
    return pos + 1
end

-------------------------------------
--
--
--
--
local function do_bounds(q, tokens, pos, n)
    table.insert(q.jstab, '"bounds":')

    local t = tokens[pos]
    if t[CD] ~= LITERAL then error("Literal expected") end
    local s = ' "' .. t[TT] .. '"'
    table.insert(q.jstab, s)

    table.insert(q.jstab, ', ')

    if pos == n then return nil end
    return pos + 1
end

-------------------------------------
--
--
--
--
local function do_where_clause(q, tokens, pos, n)
    table.insert(q.jstab, '[')
    
    local t, s
    local ident

    -- ident
    t = tokens[pos]
    if t[CD] ~= LITERAL then error("Literal expected") end
    ident = t[TT]

    local v = current_from.options[ident]
    if v then
        q.option = true
    elseif current_from.selectors[ident] then
        q.terminal = true
    else
        err_unk_where_field(ident)
    end
    s = ' "' .. ident .. '"'
    table.insert(q.jstab, s)
    if pos == n then error("Incomplete where clause: missing where field") end
    pos = pos + 1

    -- function
    t = tokens[pos]
    if t[CD] ~= LITERAL then error("Literal expected") end
    s = ', "' .. t[TT] .. '"'
    table.insert(q.jstab, s)
    if pos == n then error("Incomplete where clause: missing where function") end
    pos = pos + 1

    -- arguments
    local n_args = 0
    local found_and = false

    while true do
        t = tokens[pos]
        local v = t[CD]
        if v == STRING then
            -- deveria mapear strings para literais
            v = LITERAL
        end

        if v ~= LITERAL then error("Incomplete where clause: missing value") end
        s = ', ' .. t[TT]
        table.insert(q.jstab, s)
        n_args = n_args + 1

        if pos == n then
            pos = nil
            break
        end
        pos = pos + 1

        t = tokens[pos]
        if t[CD] == AND then
            found_and = true
            pos = pos + 1
            break
        end
        
        if t[CD] ~= COMMA then break end
        pos = pos + 1
    end            
    
    table.insert(q.jstab, ']')
    return pos, found_and
    
end    

-------------------------------------
--
--
--
--
local function do_where(q, tokens, pos, n)
    table.insert(q.jstab, '"where": [')

    while true do 
        local found_and
        pos, found_and = do_where_clause(q, tokens, pos, n)
        if not pos then break end
        if not found_and then break end
        table.insert(q.jstab, ', ')
    end
        
    table.insert(q.jstab, '], ')

    return pos
end


-------------------------------------
--
--
--
--
local function do_group_by(q, tokens, pos, n)
    table.insert(q.jstab, '"group by": [')
    local t
    while true do
        t = tokens[pos]
        if t[CD] ~= LITERAL then error("Literal expected") end

        local s = ' "' .. t[TT] .. '"'
        table.insert(q.jstab, s)
        if pos == n then pos=nil; break end
        pos = pos + 1


        t = tokens[pos]
        if t[CD] ~= 16 then break end
        table.insert(q.jstab, ', ')
    end

    table.insert(q.jstab, '], ')

    return pos
end

-------------------------------------
--
--
--
--
local function do_group_by_2(q, tokens, pos, n)
    local t = tokens[pos]
    if t[CD] ~= BY then error("'by' expected") end
    return do_group_by(q, tokens, pos+1, n)
end

-------------------------------------
--
--
--
--
local function do_output(q, tokens, pos, n)
    table.insert(q.jstab, '"output": [')
    local t
    while true do
        t = tokens[pos]
        if t[CD] ~= LITERAL then error("Literal expected") end
        local s = ' "' .. t[TT] .. '"'
        table.insert(q.jstab, s)
        if pos == n then pos=nil; break end
        pos = pos + 1

        t = tokens[pos]
        if t[CD] ~= COMMA then break end
        table.insert(q.jstab, ', ')
    end

    table.insert(q.jstab, '], ')
    return pos
end

---------------------------------------------------------



--------------------------------------------------------

local map = {
    select    =     { SELECT, do_select },
    where     =     { WHERE, do_where },
    ["group-by"]  = { GROUP_BY, do_group_by },
    ["group"]  =    { GROUP_BY, do_group_by_2 },
    ["by"]    =     { BY, nil },
    output    =     { OUTPUT, do_output },
    from      =     { FROM, do_from },
    ["and"]       = { AND, nil },
    [","]         = { COMMA, nil },
    bounds    =     { BOUNDS, do_bounds }
}

---------------------------------------------------------



--------------------------------------------------------

-------------------------------------
--
--
--
--
function string:split(sep)
   local sep, fields = sep or " ", {}
   local pattern = string.format("([^%s]+)", sep)
   self:gsub(pattern, function(c) fields[#fields+1] = c end)
   return fields
end

-------------------------------------
--
--
--
--
local function tokenize(str)
    local sqls = {}
    local toks = {}
    local first, last = 0, 0
    local p

    while true do
        p, last = str:find('[",;]', first+1)
        local finish = not p
        p = p or 0
        local plain = str:sub(first+1, p-1)

        local fields = plain:split(' ')
        for _, f in ipairs(fields) do
            table.insert(toks, { 0, f } )
        end
        if finish then break end

        local c = str:sub(p,p)
        if c == ';' then
            table.insert(sqls, toks)
            toks = {}
        elseif c == ',' then 
            table.insert(toks, { COMMA, ',' } )
        else 
            local s0 = p
            p, last = str:find('"', s0+1)
            if not p then error("Unterminated string") end
            local sql_str = str:sub(s0,p)
            table.insert(toks, { STRING, sql_str } )
        end
        first = p
    end
    table.insert(sqls, toks)

    for _, toks in ipairs(sqls) do
        for i, t in ipairs(toks) do
            if t[CD] ~= LITERAL and t[CD] ~= STRING then
                local tok = t[TT]
                local m = map[tok]
                if not m then 
                    t[CD] = LITERAL
                else
                    t[CD] = m[1]
                    t[DO] = m[2]
                end
            end
        end
    end
    return sqls
end

-------------------------------------
--
--
--
--
local function print_tokens(toks)
    
    for i, t in ipairs(toks) do
        print(t[CD], t[TT])
    end
end

-------------------------------------
--
--
--
--
local function compile_tsql_query(parts, pos)
    local q = { jstab = { "{" } }
    local n = #parts
    while true do
        local t = parts[pos]
        local do_fun = t[DO] 
        if not do_fun then error() end
        pos = pos + 1
        pos = do_fun(q, parts, pos, n)
        if not pos or pos == n then break end
    end
    table.insert(q.jstab, ' "end": 1}')

    print("no errors")
    return table.concat(q.jstab, "")
end

-------------------------------------
--
--
--
--
local function compile_tsql(str)
    local pos = 1
    local sqls = tokenize(str)
    for _, parts in ipairs(sqls) do
        print ("-----------------------")
        local jsonstr = compile_tsql_query(parts, pos)
        print (jsonstr)
    end
    return true
end    


---------------------------------------------------------
--
-- Test
--
--------------------------------------------------------
local str = 'from ufes select rtt_ns_avg where time between "10", "20" and location zrect -12.13,12.1,121, 33 group by z; bounds time'
print(str)

local status, json_str = pcall(compile_tsql, str)
if not status then
    print("err:", json_str)
end

