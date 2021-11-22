local _m = {}

local ngx = ngx or nil

------------------------------------------------------------------------------ 
--
-- MAP URL
--
------------------------------------------------------------------------------ 

local default_url = "http://127.0.0.1:9001"

local from_to_url = {
    rnp_ttls = "http://127.0.0.1:9001",
    perfsonar = "http://127.0.0.1:9105",
}

-------------------------------------
--
--
--
--
function _m.map_from_to_url(from)
    return  from_to_url[from] or default_url
end


------------------------------------------------------------------------------ 
--
-- post_request
--
------------------------------------------------------------------------------ 
local http = require "resty.http"
local httpc = http.new() 
httpc:set_timeout(60000)

-------------------------------------
--
-- Faz o request
--
function _m.post_request(url, body, options)
    local res, err = httpc:request_uri(
    	url, {
    	method = "POST",
    	body = body,
    --	headers = {
    --		--        	["Content-Type"] = "application/x-www-form-urlencoded",
    --	},
    	keepalive_timeout = 60000,
    	keepalive_pool = 10
    }) 

    --
    -- Verifica a ocorrencia de algum erro
    --
    if not res then
        ngx.log(ngx.ERR,"failed to request: "..err) 
        ngx.say("failed to request: ", err)
        return
    end 
    
    return res
end



return _m