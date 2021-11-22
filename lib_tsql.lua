local tsfilters = require "lib_tsql_filters"
local tc_hosts  = require "lib_tc_hosts"
local tc        = require "lib_tc_functions"

local _m = {}

-----------------------------------------------------------------
--                 Group By Keywords
-----------------------------------------------------------------

local PMIN, PMAX, PMAP, PHEAD, PTAIL = 1, 2, 3, 4, 5

local group_by_kws = {
    by         = { [PMIN]=1 },
    get        = { [PMIN]=3 },
    length     = { [PMIN]=1, [PMAX]=1 },
    binsize    = { [PMIN]=1, [PMAX]=1 },
    start_time = { [PMIN]=1 },
    end_time   = { [PMAX]=1 },
}

-----------------------------------------------------------------
--                 Types of Queries
-----------------------------------------------------------------
--[[
    valid combinations using schema:
    get schema

    valid combinations using bounds:

    bounds <option>
    bounds <option> where <option>
    bounds <option> where <selector>
    bounds <option> where <options> and <selector>

    bounds <selector>
    bounds <selector> where <option>
    bounds <selector> where <selector>
    bounds <selector> where <options> and <selector>

    valid combinations using select <t-measure>:

    select <t-measure>

    select <t-measure> where <option-filters>
    select <t-measure> group by <option>
    select <t-measure> where <option-filters> group by <option>

    select <t-measure> where <selector-filter>
    select <t-measure> group by <option>
    select <t-measure> where <selector-filter>  group by <option>

    select <t-measure> where <option-filters> and <selector-filter>
    select <t-measure> where <option-filters> and <selector-filter> group by <option>


    valid combinations using select <s-measure>:

    select <s-measure> where <option-filters>
    select <s-measure> where <selector-filter>
    select <s-measure> where <option-filters> and <selector-filter>

    select <s-measure> group by <selector>

    select <s-measure> where <option-filters> group by <selector>
    select <s-measure> where <selector-filter> group by <selector>
    select <s-measure> where <option-filters> and <selector-filter> group by <selector>

    NOT valid combinations using select <s-measure>:

    select <s-measure> group by <option> (???)
    select <s-measure> where <option-filters> group by <option>
    select <s-measure> where <selector-filter> group by <option>
    select <s-measure> where <option-filters> and <selector-filter> group by <option>


]]---------------------------------------------------------------

local LITERAL = 1
local STRING = 2

local SELECT = 10
local WHERE = 11
local GROUP_BY = 12
local OUTPUT = 13
local USE = 14
local BOUNDS = 17
local AND = 15
local COMMA = 16
local BY = 19
local INTO = 20
local RETURN = 21

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
local function err_unk_measures(q, ident)
    local s = ""
    for k, v in pairs(q.common.use_table.items.t_measures) do
        s = s .. k .. " "
    end
    for k, v in pairs(q.common.use_table.items.s_measures) do
        s = s .. k .. " "
    end
    error("Unknown measure: "..ident..". Valid measures are: { " .. s .. " }")
end

-------------------------------------
--
--
--
--
local function err_unk_where_field(q, ident)
    local s = ""
    for k, v in pairs(q.common.use_table.items.options) do
        s = s .. k .. " "
    end
    for k, v in pairs(q.common.use_table.items.selectors) do
        s = s .. k .. " "
    end
    error("Unknown where-field: "..ident..". Valid values are: { " .. s .. " }")
end

-------------------------------------
--
--
--
--
local function err_unk_bounds_field(q, ident)
    local s = ""
    for k, v in pairs(q.common.use_table.items.options) do
        s = s .. k .. " "
    end
    for k, v in pairs(q.common.use_table.items.selectors) do
        s = s .. k .. " "
    end
    error("Unknown where-field: "..ident..". Valid values are: { " .. s .. " }")
end


-------------------------------------
--
--
--
--
local function do_use (q, tokens, ntok, n)
    if q.use then error("Using 'use' twice in a single query") end
    local t = tokens[ntok]
    if t[CD] ~= LITERAL then error("Literal expected but found: " .. t[TT]) end
    local ident = t[TT]
    local use_ident = q.common.use_table[ident]
    if not use_ident then error ('Unknown from: ' .. ident) end
	q.common.sel_use = use_ident


--[[
    local s = ' "' .. ident .. '"'
    table.insert(q.jstab, '"from":')
    table.insert(q.jstab, s)
    table.insert(q.jstab, ', ')
    q.common.use = true
--]]
    q.common.use = '"from": "' .. ident .. '"'
    q.use = true

    if ntok == n then return nil end
    return ntok + 1
end


-------------------------------------
--
--
--
--
local function do_select (q, tokens, ntok, n)
    if not q.common.use then error("You must have selected a tinycubes with 'use'") end
    if q.schema then error("You cannot use 'select' clause with 'schema' clause") end
    if q.bound then error("You cannot use 'select' clause with 'bounds' clause") end
    if q.select then error("You cannot use 'select' clause twice on a query") end
    q.select = true
    q.someStat = true

    table.insert(q.jstab, '"select": [')
    local t
    while true do
        t = tokens[ntok]
        if t[CD] ~= LITERAL then error("Literal expected") end
        local ident = t[TT]
        if q.common.sel_use.items.s_measures[ident] then
            q.sel_s_m = 1
        elseif q.common.sel_use.items.t_measures[ident] then
            q.sel_t_m = 1
        else
            err_unk_measures(q, ident) 
        end
        if q.bounds then error("query cannot use 'bounds' and 'select'") end
        q.select=1
        local s = '"' .. ident .. '"'
        table.insert(q.jstab, s)
        if ntok == n then ntok = nil; break end
        ntok = ntok + 1

        t = tokens[ntok]
        if t[CD] ~= COMMA then break end
        table.insert(q.jstab, ', ')
    end            
    table.insert(q.jstab, '], ')

    return ntok
end
    
-------------------------------------
--
--
--
--
local function do_bounds(q, tokens, ntok, n)
    if not q.common.use then error("You must have selected a tinycubes with 'use'") end
    if q.schema then error("You cannot use 'bounds' clause with 'schema' clause") end
    if q.select then error("You cannot use 'bounds' clause with 'select' clause") end
    if q.bounds then error("You cannot use 'bounds' clause more than once") end
    q.someStat = true

    table.insert(q.jstab, '"bounds":')

    local t = tokens[ntok]
    if t[CD] ~= LITERAL then error("Literal expected") end

    local ident = t[TT]
    if q.common.sel_use.items.selectors[ident] then
        q.bounds_S = 1
    elseif q.common.sel_use.items.options[ident] then
        q.bounds_O = 1
    else
        err_unk_bounds_field(q, ident)
    end
    if q.select then error("query cannot use 'bounds' and 'select'.") end
    q.bounds=1

    local s = ' "' .. ident .. '"'
    table.insert(q.jstab, s)

    table.insert(q.jstab, ', ')

    if ntok == n then return nil end
    return ntok + 1
end

-------------------------------------
--
--
--
--
local function do_where_clause(q, tokens, ntok, n)
    table.insert(q.jstab, '[')
    
    local t, s
    local ident

    -- ident
    t = tokens[ntok]
    if t[CD] ~= LITERAL then error("Literal expected but found: " .. t[TT]) end
    
    ident = t[TT]
    if q.select_fields[ident] then error('Trying to create a second filter using the same field: '..ident) end
    q.select_fields[ident] = true

    local map_cat
    local ident_option = q.common.sel_use.items.options[ident]
    if ident_option then
        map_cat = type(ident_option) == "table"
        q.where_O = true
    elseif q.common.sel_use.items.selectors[ident] then
        q.where_S = true
    else
        err_unk_where_field(q, ident)
    end
    s = ' "' .. ident .. '"'
    table.insert(q.jstab, s)
    if ntok == n then error("Incomplete where clause: missing where field") end
    ntok = ntok + 1

    -- op-filter
    t = tokens[ntok]
    if t[CD] ~= LITERAL then error("Literal expected") end
    local filter_name = t[TT]
    local filter_info = q.common.option_filters[filter_name]
    local min_args = filter_info.vmin
    local max_args = filter_info.vmax
    local head_fmt = filter_info.vhead
    local tail_fmt = filter_info.vtail
    if not filter_info then error("Unknown option filter: ".. filter_name) end
    s = ', "' .. filter_name .. '"'
    table.insert(q.jstab, s)
    if ntok == n then error("Incomplete where clause: missing where function") end
    ntok = ntok + 1

    -- filter-arguments
    local n_args = 0
    local found_and = false

    while true do
        t = tokens[ntok]
        local v = t[TT]
        local cd = t[CD]

        if cd == LITERAL then
            if not tonumber(v) then
                -- print(ident, ident_option, v)
                if not (ident_option and type(ident_option) == "table") then error("There is no map rule for '"..ident.."' to map ".. v) end
                local new = ident_option.map_cat[v] 
                if not new then error("Could not map ["..v.."] for ident '"..ident.."'") end
                v = new
            end        
        elseif cd == STRING then
            local f = filter_info.vmap
            if type(f) == "function" then
                v = tostring(f(v))
            elseif map_cat then
                local unquoted = v:sub(2,#v-1)
                v = ident_option.map_cat[unquoted] or v
            end
        else
            error("Incomplete where clause: missing a literal value") 
        end

        n_args = n_args + 1
        if max_args and n_args > max_args then error(string.format("Too many filter arguments for function '%s'. Max: %d Found %d ", filter_name, max_args, n_args)) end

        local fmt = string.sub(head_fmt, n_args, n_args) or string.sub(tail_fmt, 1, 1)
        if fmt == 'i' and string.find(v, '%.') then error("Invalid float point parameter '"..v.."' at argument"..n_args.." of '"..ident.."'") end
        
        s = ', ' .. v
        table.insert(q.jstab, s)


        if ntok == n then ntok = nil; break end
        ntok = ntok + 1       -- consumes literal

        t = tokens[ntok]

        -- and keyword?
        if t[CD] == AND then
            found_and = true
            ntok = ntok + 1    -- consumes AND
            break            -- stop this clause 
        end
        
        -- not "," ? stop!
        if t[CD] ~= COMMA then break end
        ntok = ntok + 1        -- consumes ","
    end            
    
    if min_args and n_args < min_args then error(string.format("Too few filter arguments for function '%s'. Min: %d Found %d ", filter_name, min_args, n_args)) end

    table.insert(q.jstab, ']')
    return ntok, found_and
    
end    

-------------------------------------
--
--
--
--
local function do_where(q, tokens, ntok, n)
    if not q.select and not q.bound then error("'where' clause requires previous either 'select' or 'bounds' clause") end
    if q.schema then error("You cannot use 'where' clause with 'schema' clause") end
    if q.where then error("You cannot use 'where' clause twice in the same query") end
    -- if not q.common.use then error("You must have selected a tinycubes with 'use'") end

    q.select_fields = {}

    table.insert(q.jstab, '"where": [')

    while true do 
        local found_and
        ntok, found_and = do_where_clause(q, tokens, ntok, n)
        if not ntok then break end
        if not found_and then break end
        table.insert(q.jstab, ', ')
    end
        
    table.insert(q.jstab, '], ')

    return ntok
end


-------------------------------------
--
--
--
--
local function do_group_by_head(q, tokens, ntok, n)
    
    if not q.select then error("'group by' clause requires a previous 'select' clause") end
    if q.bounds then error("You cannot use 'group by' clause with 'bound' clause") end
    if q.schema then error("You cannot use 'group by' clause with 'schema' clause") end
    if q.group_by then error("You cannot use 'group-by' clause more than once") end
    if not q.common.use then error("You must have selected a tinycubes with 'use'") end

    local t
    while true do
        t = tokens[ntok]
        if t[CD] ~= LITERAL then error("Literal expected but found: " .. t[TT]) end

        local ident = t[TT]
        if q.common.sel_use.items.selectors[ident] then
            if q.group_by and q.group_by ~= 'S' then  error("Mixing different kinds of sources in 'group by'") end
            --if q.where_S then  error("A where clause using a selector cannot coexist with a group by over a selector") end
            q.group_by = 'S'
        elseif q.common.sel_use.items.options[ident] then
            if q.group_by and q.group_by ~= 'O' then  error("Mixing different kinds of sources in 'group by'") end
            --if q.where_O then  error("A where clause using an option cannot coexist with a group by over an option") end
            q.group_by = 'O'
        else
            err_unk_bounds_field(q, ident)
        end
    
        local s = ' "' .. ident .. '"'
        table.insert(q.jstab, s)
        if ntok == n then ntok=nil; break end
        ntok = ntok + 1

        t = tokens[ntok]
        if t[CD] ~= COMMA then break end
        ntok = ntok + 1

        table.insert(q.jstab, ', ')
    end

    return ntok
end

-------------------------------------
--
--
--
--
local function parse_list(tokens, ntok, n)
    -- filter-arguments
    local n_args = 0
    local args = {}

    while true do
        local t = tokens[ntok]
        local v = t[CD]
        if v == STRING then
            -- deveria mapear strings para literais
            v = LITERAL
        end

        if v ~= LITERAL then error("Incomplete list: missing literal value.  Found:"..t[TT]) end
        table.insert(args, t[TT])
        n_args = n_args + 1

        if ntok == n then ntok = nil; break end
        ntok = ntok + 1       -- consumes literal

        t = tokens[ntok]
        -- not "," ? stop!
        if t[CD] ~= COMMA then break end
        ntok = ntok + 1        -- consumes ","
    end
    return ntok, args
end


local giving_options = { avg='A' , min='B', max='T', median='M' }

-------------------------------------
--
--
--
--
local function do_g_as(q, tokens, ntok, n)
    local t, tt
    local options = ''
    local c = 0
    while true do 
        t = tokens[ntok]; ntok = ntok + 1
        if not t then if c > 0 then return end error('Missing giving')  end
        c = c + 1
        tt = t[TT]
        local o = giving_options[tt]
        if not o then error("Invalid 'as' option "..tt) end
        options = options .. o

        t = tokens[ntok]; if not t then return end; 
        if t[CD] ~= COMMA then break end
        ntok = ntok + 1
    end
    return ntok
end

--[[

    into x

    group by pop_id
        using 500 bins | using binsize 300
        as avg, min
        since x 
    until | last y

    call


]]--

local function tointeger(s)
    local r = tonumber(s)
    if r and not s:find('.') then return r end
end

-------------------------------------
--
--
--
--
local function do_group_by(q, tokens, ntok, n)
    local t, tt 
    t = tokens[ntok]
    if t[CD] ~= BY then error("'by' expected") end

    table.insert(q.jstab, '"group by": [')
    ntok = do_group_by_head(q, tokens, ntok+1, n)
    if ntok then
        local do_it = function()
            -- "giving" name-list
            t = tokens[ntok]
            print(1)
            if t[TT] == 'using' then
                ntok = ntok + 1

                -- next after 'using'
                t = tokens[ntok]; if not t then error("Missing bin definition")  end; tt = t[TT]
                ntok = ntok + 1

                print("tt", tt)
                -- 'binsize' $integer 
                if tt == 'binsize' then
                    t = tokens[ntok]; if not t then error("Missing binsize value") end; tt = t[TT]; ntok = ntok + 1
                    if t[CD] ~= LITERAL or tointeger(tt) == nil then error("1111") end
                    -- tointeger(tt) 
                
                -- $integer 'bins'
                elseif t[CD] == LITERAL then
                    local v = tonumber(tt)
                    if v == nil or tt:find('%.') then error('invalid bin number: '.. tt) end
                    t = tokens[ntok]; if not t then error("Missing 'bins'") end; tt = t[TT]; ntok = ntok + 1
                    if tt ~= 'bins' then error("Expecting 'bins' but found: "..tt) end

                else
                    error("Missing bin definition. Found: "..tt)
                end

                -- as ...
                t = tokens[ntok]; if not t then return end; tt = t[TT]
                if tt ~= 'as' then return end
                ntok = do_g_as(q, tokens, ntok+1, n)

                -- since value
                t = tokens[ntok]; if not t then return end; tt = t[TT]
                if tt ~= 'since' then return end
                ntok = ntok + 1

                t = tokens[ntok]; tt = t[TT]; ntok = ntok + 1
                if not t then error("Missing 'since' value") end
                -- convert date-string to epoch

                t = tokens[ntok]; if not t then return end; tt = t[TT]
                local s = tt
                if tt ~= 'until' and tt ~= 'last' then return end
                ntok = ntok + 1
                
                t = tokens[ntok]; tt = t[TT]; ntok = ntok + 1
                if not t then error("Missing 'until'/'last' value") end

                return true
            end
        end
        do_it() -- then ntok = nil end
    end
    table.insert(q.jstab, '], ')

    return ntok
end

-------------------------------------
--
--
--
--
local function do_into(q, tokens, ntok, n)
	--ngx.log(ngx.ERR,"++++++++++++++++++ into := " .. tostring("INTOTOTOTOT") .. " +++++++")
    if not q.select then error("'into' clause requires a previous 'select' clause") end
	t = tokens[ntok]; 
	if not t then error ("variable expected but received "..tt) end; 
	tt = t[TT]; ntok = ntok + 1
	if tt:sub(1,1) ~= '$' then error ("variable expected but received "..tt) end
    q.into = tt
	--ngx.log(ngx.ERR,"++++++++++++++++++ into := " .. tostring(tt) .. " +++++++")

    return ntok
end

-------------------------------------
--
--
--
--
local function do_return(q, tokens, ntok, n)
	if q.nclauses > 0 then error("return must be a unique clause") end
	t = tokens[ntok]; if not t then return end; tt = t[TT]; ntok = ntok + 1
	if tt:sub(1,1) ~= '$' then error ("variable expected but received "..tt) end
    q._return = tt
	q.someStat = true

    return ntok
end

-------------------------------------
--
--
--
--
local function do_output(q, tokens, ntok, n)
    table.insert(q.jstab, '"output": [')
    local t
    while true do
        t = tokens[ntok]
        if t[CD] ~= LITERAL then error("Literal expected") end
        local s = ' "' .. t[TT] .. '"'
        table.insert(q.jstab, s)
        if ntok == n then ntok=nil; break end
        ntok = ntok + 1

        t = tokens[ntok]
        if t[CD] ~= COMMA then break end
        table.insert(q.jstab, ', ')
    end

    table.insert(q.jstab, '], ')
    return ntok
end

---------------------------------------------------------



--------------------------------------------------------

local reserved = {
    select    = { SELECT, do_select },
    where     = { WHERE, do_where },
    ["group"] = { GROUP_BY, do_group_by },
    ["by"]    = { BY, nil },
    output    = { OUTPUT, do_output },
    use       = { USE, do_use },
    ["and"]   = { AND, nil },
    [","]     = { COMMA, nil },
    bounds    = { BOUNDS, do_bounds },
    into      = { INTO, do_into },
    ["return"]= { RETURN, do_return },
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
   local pattern = string.format("([^%s\n]+)", sep)
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
    local first = 0
    local p

    while true do
        p = str:find('[",;#]', first+1)

        local plain = str:sub(first+1, (p or 0) - 1)
        plain:gsub("([^ \t\r\n]+)", 
            function(c)
                local m, v, f = reserved[c], LITERAL, nil
                if m then v = m[1]; f = m[2] end
                table.insert(toks, { v, c, f } )
            end)
        if not p then break end

        local c = str:sub(p,p)
        if c == ',' then 
            table.insert(toks, { COMMA, ',', nil } )
        elseif c == ';' then
            if #toks > 0 then -- ignores sequences of ';'
                table.insert(sqls, toks)
                toks = {}
            end
        elseif c == '#' then
            local p0 = p
            p = str:find('\n', p0+1)
            if not p then break end
        else
            local p0 = p
            p = str:find('"', p0+1)
            if not p then error("Unterminated string") end
            table.insert(toks, { STRING, str:sub(p0,p) } )
        end
        first = p
    end
    if #toks ~= 0 then 
        table.insert(sqls, toks)
    end
    --[[ for k, v in ipairs(sqls) do 
        for k2, t in ipairs(v) do
            print(k2,t[TT])
        end
    end]]--
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
local function compile_tsql_query(common, tokens, ntok)
    local q = { common = common, jstab = { '{  "id":100, ' }, nclauses = 0 }
    local n = #tokens
    while true do
        local t = tokens[ntok]
        if not t then break end
        local do_fun = t[DO]
        if not do_fun then error("Unknown clause: '" .. t[TT] .. "' " ) end
        ntok = ntok + 1
        ntok = do_fun(q, tokens, ntok, n)
        if not ntok or ntok == n then break end
		q.nclauses = q.nclauses + 1
    end
    if not q.someStat then return end
    table.insert(q.jstab, q.common.use)
    table.insert(q.jstab, ' }')
	local query_js = table.concat(q.jstab, "")
	local t = { js = query_js, _into = q.into, _return = q.do_return }
	return t
end

-------------------------------------
--
--
--
--
function _m.compile_tsql(source, param_use_table)
	local queries = {}
    
	local queries_tokens = tokenize(source)
	
    if #queries_tokens == 0 then 
        table.insert(queries, jsError("Request without queries"))
    else
		local ntok = 1
        local common = {src = source, use_table = tc_hosts.hosts, option_filters = tsfilters.option_filters}
		for _, tokens in ipairs(queries_tokens) do
			local ok, js_table = pcall (compile_tsql_query, common, tokens, ntok)
			if not ok then 
				table.insert(queries, { err = tc.buildError(js_table) } ) -- js_query has the err string
			elseif js_table then 
				table.insert(queries, js_table)
			else
				-- there was no actual statement in the query
			end
		end
	end
    return queries
end


return _m
