dofile("table_show.lua")
dofile("urlcode.lua")
local urlparse = require("socket.url")
local http = require("socket.http")
local zip = require("zip")
JSON = (loadfile "JSON.lua")()

local item_dir = os.getenv('item_dir')
local warc_file_base = os.getenv('warc_file_base')
local item_type = os.getenv('item_type')
local item_name = os.getenv('item_name')
local item_value = os.getenv('item_value')

if urlparse == nil or http == nil then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false
local outlinks = {}

local ids = {}
ids[item_value] = true

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end

abort_item = function(item)
  abortgrab = true
end

load_json_file = function(file)
  if file then
    return JSON:decode(file)
  else
    return nil
  end
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

allowed = function(url, parenturl)
  for s in string.gmatch(url, "([0-9]+)") do
    if ids[s] then
      return true
    end
  end

  if string.match(url, "^https?://s%.gjcdn%.net")
    or string.match(url, "^https?://m%.gjcdn%.net/user%-avatar/")
    or string.match(url, "^https?://m%.gjcdn%.net/game%-header/") then
    return false
  end

  if string.match(url, "^https?://[^/]*gjcdn%.net")
    or string.match(url, "^https?://[^/]*gamejolt%.net")
    or string.match(url, "^https?://[^/]*gamejolt%.com/site%-api/") then
    return true
  end
  
  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  if not processed(url) and allowed(url, parent["url"]) then
    addedtolist[url] = true
    return true
  end
  
  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  
  downloaded[url] = true

  local function check(urla, post_data)
    local origurl = url
    local url = string.match(urla, "^([^#]+)")
    local url_ = string.match(url, "^(.-)%.?$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if type(post_data) == "table" then
      post_data = JSON:encode(post_data)
      if post_data == "[]" then
        post_data = "{}"
      end
    else
      post_data = ""
    end
    if not processed(url_ .. post_data)
      and string.match(url_, "^https?://.+")
      and allowed(url_, origurl) then
      print('queuing', url_, post_data)
      if post_data ~= "" then
        table.insert(urls, {
          url=url_,
          method="POST",
          body_data=post_data
        })
      else
        table.insert(urls, {
          url=url_
        })
      end
      addedtolist[url_ .. post_data] = true
      addedtolist[url .. post_data] = true
    end
  end

  local function checknewurl(newurl)
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  if string.match(url, "^https?://[^/]*gamejolt%.net/data/games/.+%.zip$") then
    local zip_file = zip.open(file)
    local base = string.match(url, "^(.+/)")
    for filedata in zip_file:files() do
      check(base .. filedata["filename"])
    end
  end

  local function check_posts(posts)
    for _, data in pairs(posts) do
      scroll_id = data["scroll_id"]
    end
    if scroll_id then
      check("https://gamejolt.com/site-api/web/posts/fetch/game/" .. item_value, {scrollId=scroll_id, scrollDirection="from"})
    end
  end

  if allowed(url) and status_code < 300
    and not string.match(url, "^https?://[^/]*gjcdn%.net")
    and not string.match(url, "^https?://[^/]*%.gamejolt%.net") then
    html = read_file(file)
    if string.match(url, "^https?://[^/]*gamejolt%.com/site%-api/web/discover/games/overview/[0-9]+%?ignore") then
      check(string.match(url, "^([^%?]+)"))
      check("https://gamejolt.com/site-api/web/discover/games/" .. item_value)
      check("https://gamejolt.com/site-api/comments/Game/" .. item_value .. "/hot?page=1")
      check("https://gamejolt.com/site-api/web/download/info/" .. item_value)
      check("https://gamejolt.com/site-api/web/posts/fetch/game/" .. item_value)
      local json = JSON:decode(html)
      for _ in pairs(json["payload"]["songs"]) do
        check("https://gamejolt.com/get/soundtrack?game=" .. item_value)
        check("https://gamejolt.com/site-api/web/discover/games/audio/get-soundtrack-download-url/" .. item_value)
        break
      end
      local builds = json["payload"]["builds"]
      if builds then
        for _, data in pairs(builds) do
          check("https://gamejolt.com/site-api/web/download/info/" .. item_value .. "?build=" .. data["id"])
          check("https://gamejolt.com/get/build?game=" .. item_value .. "&build=" .. data["id"])
          check("https://gamejolt.com/site-api/web/discover/games/builds/get-download-url/" .. data["id"], {forceDownload=true})
          check("https://gamejolt.com/site-api/web/discover/games/builds/get-download-url/" .. data["id"], {})
        end
      end
      check_posts(json["payload"]["posts"])
    end
    if string.match(url, "^https?://[^/]*gamejolt%.com/site%-api/web/posts/fetch/game/") then
      local json = JSON:decode(html)
      local scroll_id = nil
      check_posts(json["payload"]["items"])
    end
    if string.match(url, "/site%-api/comments/Game/.+%?page=[0-9]+$") then
      local json = JSON:decode(html)
      if json["payload"]["count"] ~= 0 then
        local page = tonumber(string.match(url, "page=([0-9]+)$")) + 1
        check(string.match(url, "^(https?://.+%?page=)") .. tostring(page))
      end
    end
    if string.match(url, "/site%-api/web/discover/games/builds/get%-download%-url/[0-9]+$") then
      local build_id = string.match(url, "([0-9]+)$")
      local json = JSON:decode(html)
      local token = string.match(json["payload"]["url"], "^https?://[^/]*gamejolt%.net/%?token=([0-9a-zA-Z]+)")
      if token then
        check("https://gamejolt.net/site-api/gameserver/" .. token)
      end
    end
    if string.match(url, "/site%-api/gameserver/[0-9a-zA-Z]+$") then
      local json = JSON:decode(html)
      local newurl = json["payload"]["url"]
      check(newurl)
      check(string.match(newurl, "^(.+/)") .. json["payload"]["build"]["primary_file"]["filename"])
    end
    if string.match(url, "/site%-api/web/discover/games/[0-9]+$") then
      local json = JSON:decode(html)
      check("https://gamejolt.com/games/" .. json["payload"]["game"]["slug"] .. "/" .. item_value)
    end
    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]
  
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end
  
  if (status_code >= 200 and status_code <= 399) then
    downloaded[url["url"]] = true
    downloaded[string.gsub(url["url"], "https?://", "http://")] = true
  end

  if abortgrab then
    return wget.actions.ABORT
  end
  
  if status_code >= 500
      or (status_code >= 400 and status_code ~= 404)
      or status_code  == 0 then
    io.stdout:write("Server returned " .. http_stat.statcode .. " (" .. err .. "). Sleeping.\n")
    io.stdout:flush()
    local maxtries = 8
    if not allowed(url["url"]) then
        maxtries = 0
    end
    if tries >= maxtries then
      io.stdout:write("\nI give up...\n")
      io.stdout:flush()
      tries = 0
      if allowed(url["url"]) then
        return wget.actions.ABORT
      else
        return wget.actions.EXIT
      end
    end
    os.execute("sleep " .. math.floor(math.pow(2, tries)))
    tries = tries + 1
    return wget.actions.CONTINUE
  end

  tries = 0

  local sleep_time = 0

  if sleep_time > 0.001 then
    os.execute("sleep " .. sleep_time)
  end

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local items = nil
  for item, _ in pairs(outlinks) do
    print('found item', item)
    if items == nil then
      items = item
    else
      items = items .. "\0" .. item
    end
  end
  if items ~= nil then
    local tries = 0
    while tries < 10 do
      local body, code, headers, status = http.request(
        "http://blackbird-amqp.meo.ws:23038/urls-t05crln9brluand/",
        items
      )
      if code == 200 or code == 409 then
        break
      end
      io.stdout:write("Could not queue items.\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    if tries == 10 then
      abort_item()
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if abortgrab then
    return wget.exits.IO_FAIL
  end
  return exit_status
end
