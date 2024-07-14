local ffi = require("ffi")
local C = ffi.C
ffi.cdef[[
    bool DeleteSavegame(const char* filename);
]]

local Pipe = {}
Pipe.__index = Pipe

-- Creates a socket object with the expected functions write(message) close() read()
function Pipe:create(L, pipe_name)
    local pipe = {
        log = function(msg)
            if pipe.L.debug.print_to_log then
                DebugError("[Linux_Pipe][" ..pipe.socket_filename .."] "..msg)
            end
        end

    }

    -- keep a reference to upstream errors
    setmetatable(pipe, Pipe)

    pipe.name = pipe_name
    pipe.read_unix_socket_co = false
    pipe.L = L
    pipe.server = L.sockets[pipe_name].server
    pipe.clients = L.sockets[pipe_name].clients
    pipe.last_no_client_time = nil
    pipe.socket_filename = L.sockets[pipe_name].socket_filename
    pipe.socket = require("socket.core")

    pipe.server:settimeout(0)
    -- log helper
    pipe.log = function(msg)
        if pipe.L.debug.print_to_log then
            DebugError("[Linux_Pipe][" ..pipe.socket_filename .."] "..msg)
        end
    end

    -- shutdown helper for a client, called from various places when a client is detected as dead
    pipe.shutdown = function(index)
        CallEventScripts("directChatMessageReceived", "LinPipe: Lost a connection :(")
        local client = pipe.clients[index]
        local nrec, nsent, timediff = client:getstats()
        pipe.log("Shutting down " ..tostring(client) .." after bytes rec " ..tostring(nrec) .." , bytes sent " ..tostring(nsent) ..", connection age: " ..tostring(timediff) .."s")
        client:shutdown("both")
        table.remove(pipe.clients, index)
    end

    if pipe.read_unix_socket_co then
        pipe.log('read_unix_socket_co exists!!11')
    end
    
    pipe.read_unix_socket_co = coroutine.create(function()
        local status = "closed"
        -- we checked and cached that in linpipe.lua
        local server = L.sockets[pipe.name].server
        pipe.log("Preparing new coroutine for server " ..pipe.name)

        -- Return a key with the given value (or nil if not found).  If there are
        -- multiple keys with that value, the particular key returned is arbitrary.
        function indexOf(tbl, value)
            for k, v in pairs(tbl) do
                if v == value then return k end
            end
            return nil
        end

        while true do
            status = ""
            writable_sockets = {}
            -- err: timeout|closed|nil

            -- Important: pipe x4_python_host reads all the time
            -- Other pipes get only a read when they signalled a $Start_Reading cue

            -- So if we want to accept new connections for other pipes we have to
            --  piggyback on x4_python_host read request
            if pipe.name == "x4_python_host" then
                for i, socket in pairs(L.sockets) do 
                    new_client, err = socket.server:accept()

                    if err ~= nil then
                        -- No new connection :(
                        -- pipe.log("No new connection on " ..L.sockets[pipe_name].server:getsockname())
                    elseif new_client ~= nil then
                        -- Yay new connection :)
                        pipe.log("New connection " ..tostring(new_client) .." for " ..socket.socket_filename)
                        CallEventScripts("directChatMessageReceived", "LinPipe: Accepted new connection :)")
                        if pcall(assert, new_client:settimeout(0)) then
                            table.insert(socket.clients, new_client)
                            pipe.log("Accepted new connection "..tostring(new_client))
                        else
                            pipe.log("Failed to accept new connection "..tostring(new_client))
                        end
                        -- have a break so other things have a chance to catch up
                        coroutine.yield(status)
                    end
                end
            end

            -- read becomes never available for unix type but write looks good
            -- so if we can write we assume we can also read

            -- socket:select really does not like an empty table here
            if #pipe.clients > 0 then
                -- The returned tables are doubly keyed both by integers and also by the sockets themselves
                readable_sockets, writable_sockets, err = pipe.socket:select(pipe.clients, nil, 0.01)
                if err ~= nil then
                    pipe.log("No client available for reading: "..err)
                end
            end
            
            -- pipe.log("Got " ..#writable_sockets .." writable sockets")

            for k, _ in pairs(writable_sockets) do 
                -- make sure we do access the socket
                if type(k) ~= "number" and pcall(assert, k:settimeout(0)) then
                    index = indexOf(pipe.clients, k)
                    -- pipe.log("Read from client " ..tostring(k) .." having index "..tostring(index))
                    client_msg, err = k:receive("*l")

                    if client_msg and string.len(client_msg) > 0 then 
                        status = client_msg
                        pipe.log("Read "..tostring(string.len(status)) .." characters from client " ..tostring(k))
                        coroutine.yield(status)
                    end
                end
                

                -- we do timeout all the time when the client has nothing to send
                -- so do not set timeout as status for error _here_
                -- this means also that clients that disconnected hard may still linger
                -- using up file descriptors but we will catch that one on send and remove
                -- it there
                if err ~= nil and err ~= "timeout" then
                    if err == "closed" then
                        pipe.shutdown(index)
                    else
                        -- Got an unknown error - not timeout or closed
                        status = err
                    end
                end
            end

            -- pipe.log("Yielding status " ..status) 
            coroutine.yield(status)
        end
      end)

      L.SetLastError(L.ERROR_PIPE_CONNECTED)
    -- coroutine.resume(pipe.read_unix_socket_co)
    return pipe
end

-- pipes[pipe_name].file:write()
-- returns written bytes, `Pipes.lua` is happy as long as this is > 0
function Pipe:write(message)
    if message == "garbage_collected" then
        self.log('[GC] Removing server socket coroutine')
        -- we're never in time to send this to any client before we're killed hard:
        -- remove unix_socket so it gets garbage collected
        self.read_unix_socket_co = nil
        self.clients = null
        self.server = null 

        -- filename is _without_ file extension for C.DeleteSavegame
        local is_delected = C.DeleteSavegame(self.socket_filename)
        if is_delected then
            self.log('[GC] Deleted socket '..self.socket_filename)
        end

        return 0
    end

    -- self.log("Poking WRITE coroutine: "..self.name)

    for k, client in pairs(self.clients) do
        -- In case of error, the method returns nil, followed by an error message,
        -- followed by the index of the last byte within [i, j] that has been sent.
        self.log("Writing to client " ..tostring(client))
        local err count = client:send(message)
        
        if err ~= nil then
            self.log('FATAL send failed: ' ..tostring(count) .."/" ..tostring(err))
        end

        if count ~= nil and count > 0 then
            self.log("Wrote " ..tostring(string.len(message)) .." characters to client "..tostring(client))
        else
            -- Happens when client disconnected,
            -- shutting down and freeing client for GC
            self.shutdown(k)
        end
    end

    -- fake len sent - we are the server and don't want to get teared down even when if no clients are connected
    return string.len(message)
end

-- pipes[pipe_name].file:read()
-- returns text, or empty or [nil, error_message].
function Pipe:read()
    local result = {}
    -- must NOT return empty string or UI Event: Named_Pipes will trigger in a loop
    result[0] = nil
    result[1] = "CANCELLED"

    -- pause for a while if no clients existed
    if self.last_no_client_time ~= nil then
        -- sleeping, but don't wait too long or clients have no chance of reconnecting
        if GetCurRealTime() - self.last_no_client_time > 2 then
            -- treshold reached, waking and try again on next read
            self.last_no_client_time = nil
        end
        return result[0]
    end

    status = coroutine.status(self.read_unix_socket_co)

    if status == "dead" then
        -- this recovers nicely and the game keeps going
        if self.L.last_error ~= self.L.ERROR_IO_PENDING then
            self.log("Coroutine status: "..status)
            self.L.SetLastError(self.L.ERROR_IO_PENDING)
        end
        result[1] = "ERROR"
        return result
    end

    -- self.log("Poking READ coroutine: "..self.name)

    local is_running, data = coroutine.resume(self.read_unix_socket_co)

    if is_running then
        if #self.clients == 0 and self.L.last_error == self.L.ERROR_NO_DATA then
            self.log("Having no more clients left :(")
            self.L.SetLastError(self.L.ERROR_NO_DATA)
            self.last_no_client_time = GetCurRealTime()
            -- Pipes goes all bonkers if we return with error here
            -- return result
        else
            self.L.SetLastError(self.L.ERROR_PIPE_LISTENING)
        end
        -- do not send timeout|closed - we _are_ the socket so we don't want this
        -- to be terminated if nobody is connected 
        -- https://github.com/lunarmodules/luasocket/issues/225
        -- The 'closed' status from receive() call isn't actually an error in a sense, because the
        -- nature of full-duplex streaming connections allows to close either sending or receiving
        -- half of socket without affecting the other. Once you get 'closed' it means that your peer
        -- finished sending its request and waits for a response to arrive.
        if data and string.len(data) ~= 0 and data ~= "timeout" and data ~= "closed" then
            result[0] = data
        end
    else
        self.log("FATAL coroutine died")
        result[1] = "ERROR"
        return result
    end

    return result[0]
end

-- pipes[pipe_name].file:close()
-- returns [success, message]
function Pipe:close()
    if #self.clients > 0 then
        self.log("Closing clients")
        for k, client in pairs(self.clients) do
            self.shutdown(k)
        end
    end

    self.clients = {}

    if pcall(self.server:close()) then
        self.log("Closed server")
        self.server:close()
        self.server = nil
    end
    
    -- TODO: delete pipe here?
    -- C.DeleteSavegame("x4_socket")

    self.read_unix_socket_co = nil

    local result = {}
    result[0] = SUCCESS
    result[1] = "Pipe closed"
    return result
end

return Pipe