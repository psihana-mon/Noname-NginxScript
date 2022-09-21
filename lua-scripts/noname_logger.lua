local BatchQueue = require "batch_queue"
local json = require "JSON"
local url = require "url"
local http = require "http"
local config = require "logger_configuration"

local ngx = ngx
local tostring = tostring
local tonumber = tonumber
local concat = table.concat

local queues = {} -- one queue per unique plugin config
local parsed_urls_cache = {}

-- Parse host url.
-- @param `url` host url
-- @return `parsed_url` a table with host details:
-- scheme, host, port, path, query, userinfo
local function parse_url(host_url)
  local parsed_url = parsed_urls_cache[host_url]

  if parsed_url then
    return parsed_url
  end

  parsed_url = url.parse(host_url)
  if not parsed_url.port then
    if parsed_url.scheme == "http" then
      parsed_url.port = 80
    elseif parsed_url.scheme == "https" then
      parsed_url.port = 443
    end
  end
  if not parsed_url.path then
    parsed_url.path = "/"
  end

  parsed_urls_cache[host_url] = parsed_url

  return parsed_url
end


-- Sends the provided payload (a string) to the configured plugin host
-- @return true if everything was sent correctly, falsy if error
-- @return error message if there was an error
local function send_payload(self, conf, payload)
  local connect_timeout = config.CONNECT_TIMEOUT
  local send_timeout = config.SEND_TIMEOUT
  local read_timeout = config.READ_TIMEOUT
  local keepalive = config.KEEPALIVE
  local http_endpoint = config.ENGINE_ENDPOINT
  local proxy_url = config.PROXY_URL
  local ok, err
  local parsed_url = parse_url(http_endpoint)
  local host = parsed_url.host
  local port = tonumber(parsed_url.port)

  local httpc = http.new()
  httpc:set_timeouts(connect_timeout, send_timeout, read_timeout)
  if proxy_url ~= "" then
    if parsed_url.scheme == "http" then
      httpc:set_proxy_options({http_proxy = proxy_url})
    elseif parsed_url.scheme == "https" then
      httpc:set_proxy_options({https_proxy = proxy_url})
    end
  end

  ok, err = httpc:connect({scheme = parsed_url.scheme, host = host, port = port, ssl_verify = false})
  if not ok then
    ngx.log(ngx.ERR, "failed request to " .. host .. ":" .. tostring(port) .. ": " .. err)
    return nil, "failed to connect to " .. host .. ":" .. tostring(port) .. ": " .. err
  end

  local res, err = httpc:request({
    method = "POST",
    path = parsed_url.path,
    query = parsed_url.query,
    headers = {
      ["Host"] = parsed_url.host,
      ["Content-Type"] = "application/json",
      ["Content-Length"] = #payload,
    },
    body = payload,
  })
  if not res then
    ngx.log(ngx.ERR, "failed request to " .. host .. ":" .. tostring(port) .. ": " .. err)
    return nil, "failed request to " .. host .. ":" .. tostring(port) .. ": " .. err
  end

  -- always read response body, even if we discard it without using it on success
  local response_body = res:read_body()
  local success = res.status < 400
  local err_msg

  if not success then
    err_msg = "request to " .. host .. ":" .. tostring(port) ..
              " returned status code " .. tostring(res.status) .. " and body " ..
              response_body
    ngx.log(ngx.ERR, err_msg)
  end

  if keepalive > 0 then
    ok, err = httpc:set_keepalive(keepalive)
    if not ok then
      -- the batch might already be processed at this point, so not being able to set the keepalive
      -- will not return false (the batch might not need to be reprocessed)
      ngx.log(ngx.ERR, "failed keepalive for ", host, ":", tostring(port), ": ", err)
    end
  end
  return success, err_msg
end


local function json_array_concat(entries)
  return "[" .. concat(entries, ",") .. "]"
end


local NonameSecurityHandler = {}


function NonameSecurityHandler:log(conf)
  local msg_request_headers = ngx.req.get_headers()
  local msg_response_headers = ngx.resp.get_headers()
  local req_content_len = msg_request_headers["Content-Length"]
  local resp_content_len = msg_response_headers["Content-Length"]

  local msg_request_body = req_content_len and
                           req_content_len ~= "" and
                           tonumber(req_content_len) and
                           tonumber(req_content_len) > config.MAX_BODY_SIZE and "" or ngx.var.request_body

  local msg_response_body = resp_content_len and
                            resp_content_len ~= "" and
                            tonumber(resp_content_len) and
                            tonumber(resp_content_len) > config.MAX_BODY_SIZE and "" or ngx.var.resp_body

  local msg = {
    source = {
      type = config.TYPE,
      index = config.INDEX
    },
    ip = {
      v = "1",
      src = ngx.var.remote_addr,
      dst = ngx.var.server_addr,
    },
    tcp = {
      src = tostring(ngx.var.remote_port),
      dst = tostring(ngx.var.server_port),
    },
    http = {
      v = tostring(ngx.req.http_version()),
      request = {
        ts = ngx.req.start_time(),
        method = ngx.req.get_method(),
        url = (ngx.var.request_uri or "/"),
        headers = msg_request_headers,
        body = msg_request_body
      },
      response = {
        ts = ngx.req.start_time() + ngx.var.request_time,
        status = ngx.status,
        headers = msg_response_headers,
        body = msg_response_body,
      }
    }
  }

  local entry = json:encode(msg)
  local queue_id = "queueId1"
  local q = queues[queue_id]
  if not q then
    -- batch_max_size <==> conf.queue_size
    local batch_max_size = config.BATCH_MAX_SIZE
    local process = function(entries)
      local payload = batch_max_size == 1
                      and entries[1]
                      or  json_array_concat(entries)
      return send_payload(self, conf, payload)
    end

    local opts = {
      retry_count    = config.RETRY_COUNT,
      flush_timeout  = config.FLUSH_TIMEOUT,
      batch_max_size = config.BATCH_MAX_SIZE,
      process_delay  = config.PROCESS_DELAY,
      max_concurrent_processes = config.MAX_CONCURRENT_PROCESSES,
    }

    local err
    q, err = BatchQueue.new(process, opts)
    if not q then
      ngx.log(ngx.ERR, "could not create queue")
      return
    end
    queues[queue_id] = q
  end

  q:add(entry)
end


return NonameSecurityHandler
