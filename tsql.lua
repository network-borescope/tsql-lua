---------------------------------------------------------
--
-- Preparation
--
--------------------------------------------------------

-- reject non POST connections 
if ngx.req.get_method() ~= 'POST' then return end

-- load body to transfer area
ngx.req.read_body()

-- store the request body into variable
local post_body_str = ngx.req.get_body_data()

---------------------------------------------------------
--
-- Compile the tsql source
--
--------------------------------------------------------

local tsql = require "lib_tsql"

-- call the compiler
local ok, instructions = pcall(tsql.compile_tsql, post_body_str)

-- check for a major error
if not ok then
	-- in case of error, json_queries has the error string
	ngx.say(instructions)
	return
end

---------------------------------------------------------
--
-- Execute the compiled instructions
-- 
--------------------------------------------------------

local base = require "lib_nx_base"
local tc = require "lib_tc_functions"

local variables = {}

-- process all compiled json_queries
local responses = {}
local last_response_str
ngx.log(ngx.ERR,"--------------------------------------")
for k,v in ipairs(instructions) do
	-- ngx.log(ngx.ERR,"++++++++++++++++++ into>" .. tostring(v._into) .. " +++++++")
	if v.err then 
		table.insert(responses, v.err) -- v[1] has the compiler error
	elseif v._return then 
		local response = variables[v._return]
		if not response then
			table.insert(responses, tc.buildError('Unknonwn variable '.. v._return))
		else
			table.insert(responses, response)
		end
	else
		last_response_str = base.body_request(v.js)
		if v._into then 
			ngx.log(ngx.ERR,"++++++++++++++++++ saving var:" .. tostring(v._into) .. " +++++++")
			variables[v._into] = last_response_str
		else
			table.insert(responses, last_response_str)
		end
	end
end


---------------------------------------------------------
--
-- Prepare the final result
-- 
--------------------------------------------------------
local final_result
if #responses == 1 then
	final_result = responses[1]
else 
	-- combine all queries responses into a single array
	final_result = "[" .. table.concat(responses, ",") .. "]"
end

-- send back the final_result
ngx.header.content_type = "application/json; charset=utf-8"
ngx.say(final_result)
ngx.eof()

