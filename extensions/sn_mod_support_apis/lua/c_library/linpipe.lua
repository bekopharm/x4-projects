local ffi = require("ffi")
local C = ffi.C
ffi.cdef[[
    bool DeleteSavegame(const char* filename);
]]

-- TODO: explain how to install lua5.1-socket
-- TODO: Add some lib that provides an unlink function
-- TODO: move socket creation to Linux_Pipe - we should only check here if lib is available
-- TODO: Consider UDP instead of socket (or add an option)
-- TODO: make sure that the pipe server can be instantiated so we can raise multiple sockets

-- dnf install lua5.1-socket compat-lua-libs
-- dnf install luasocket
-- ln -s /lib64/lua/5.1/socket /path/to/X4_Foundations/game/ui/core/lualibs/
-- Important: lua 5.4 socket lib does NOT work

local Pipe = require("extensions.sn_mod_support_apis.lua.named_pipes.Linux_Pipe")
local Config = require("extensions.sn_mod_support_apis.lua.userdata.Interface")

-- Table holding lib functions to be returned, or lib params that can
-- be modified.
local L = {
    debug = {
        print_to_log = true,
    },
    socket_filename = "",
    server = nil,
    pipe = nil,
    retry_allowed = false,
    -- mimicking https://docs.microsoft.com/en-us/windows/win32/ipc/named-pipe-type-read-and-wait-modes
    -- in case Pipes checks for this
    ERROR_NO_ERROR = 0,
    ERROR_NO_DATA = 1,
    ERROR_MORE_DATA = 2,
    ERROR_IO_PENDING = 3,
    ERROR_PIPE_LISTENING = 4,
    ERROR_IO_DENIED = 5,
    ERROR_PIPE_CONNECTED = 6,
    ERROR_MISSING_LIBRARY = 100,
    last_error = 0,
}

local function log(msg)
    if L.debug.print_to_log then
        DebugError("[LinPipe]: " ..msg)
    end
end

--[[
    Why we try our best to get the socket into the savegame folder:

    It appears that any io or os function (like os.execute or os.remove)
    are unavailable for extensions so we can not simply unlink our socket
    when we have to. So to make this clean we'd have to write and compile
    our own unlink lua lib but since we're going with a vanilla socket lib
    and don't want to meddle with it we make use of a tiny loophole where
    we _can_ remove files by abusing the imported C.DeleteSavegame function.
    
    This function can indeed delete files _in_ the savegame folder. The catch
    is: has to be all lowercase, must end with .xml or .xml.gz, and
    MUST BE in the savegame folder. This is the reason we raise our socket
    right there.

    However I found no way to obstain the full path to saves from any Lua
    function so we have to ask the user for this (done in options menu for
    Named Pipes API)
]]
local function get_socket_path()
    local socket_path = nil
    local config = Config.Read_Userdata('sn_mod_support_apis', 'lin_pipe')

    if config ~= nil then
        socket_path = config.pipe_name_prefix_linux
    end

    if socket_path == nil then
        socket_path = "/tmp"
    end

    -- check for save in path name
    if socket_path:match("/save") then
        log("Lingering sockets in savegame folder are deleted automatically")
    else
        DebugError("[LinPipe] WARN: Path to savegame folder not provided, socket may linger and has to be deleted manually from "..socket_path)
        CallEventScripts("directChatMessageReceived", "LinPipe: WARN: Path to saves folder not")
        CallEventScripts("directChatMessageReceived", "LinPipe: configured, socket may linger and ")
        CallEventScripts("directChatMessageReceived", "LinPipe: has to be deleted manually!")
    end

    return socket_path..'/'
end


local function require_socket_unix()
    -- last time we checked ui/core/lualibs/?.so was in package.path
    -- so a `ln -s /lib64/lua/5.1/socket /path/to/X4_Foundations/game/ui/core/lualibs` should be fine once
    -- lua5.1-socket/compat-lua-libs are installed on the system
    if not string.find(package.cpath, "sn_mod_support_apis") then
        package.cpath = "extensions/sn_mod_support_apis/lua/c_library/?.so;?.so;"..package.cpath
    end

    local socket = require("socket.core")

    if socket == nil then
        DebugError("[LinPipe] FATAL: LuaSocket not found - is lua5.1-socket installed?")
        L.last_error = L.ERROR_MISSING_LIBRARY
    else
        if socket._DEBUG then
            log('' ..socket._VERSION ..' found, debug support is enabled')
        else
            log('' ..socket._VERSION ..' found, debug support is disabled')
        end
        CallEventScripts("directChatMessageReceived", "LinPipe: "..socket._VERSION.." found")
    end

    local socket_unix = require("socket.unix")

    if socket_unix == nil then
        log('LuaSocket.unix not found - is lua5.1-socket installed?')
        L.last_error = L.ERROR_MISSING_LIBRARY
    end

    return socket_unix
end

local function create_socket_server(socket_filename, socket_unix)
	local server = socket_unix()

    if server == nil then
        log("LuaSocket.unix could not be created - is lua5.1-socket installed?")
        L.last_error = L.ERROR_MISSING_LIBRARY
        return nil
    end

    -- delete previous socket, if any, or bind will fail
    -- filename is _without_ file extension for C.DeleteSavegame
    C.DeleteSavegame(socket_filename)
    local socket_path = get_socket_path()
    local filename = socket_path..socket_filename..".xml"
    log("Creating new socket server at " ..filename)
    CallEventScripts("directChatMessageReceived", "LinPipe: Raising socket at "..filename)

    -- create a server socket by associating with a local address (bind) followed by listen
    local ok, err = pcall(assert, server:bind(filename));
    if not ok then 
        log("Failed to bind " ..err)
        CallEventScripts("directChatMessageReceived", "LinPipe: Permission denied")
        L.last_error = L.ERROR_IO_DENIED
        return nil
    else
        L.last_error = L.ERROR_PIPE_CONNECTED
        assert(server:listen())
    end
    
	return server
end

function L.SetLastError(error_code)
    L.last_error = error_code
end

function L.GetLastError()
    return L.last_error
end

local function explode(value, sep)
    local t={}

    for str in string.gmatch(value, "([^"..sep.."]+)") do
        table.insert(t, str)
    end

    return t
end


--[[
Important: We can|want not really mimic the NamedPipe feature of X4 Python Pipe Server.
From my understanding it works by reading the "magic" file `pipe_external.txt` of each
module reported by X4 on connect and simply executes it's content. This kinda results in
additional pipes (or not) depending on source in each extension. This links all _very hard_
not only to Python but also to Windows only and this is IMHO a very bad approach for a socket
that should be by design agnostic of the language, tool, framework or even OS that connects to
it. So what we do is kinda open a socket ourself and allowing anyone interested to connect.

This has some drawbacks. For example we multiply the send data on each connection and it's not
said that external apps will be able to deal with data that was not supposed for them in the
first place (error handling).

On reading we kinda will have to build a local buffer that is emptied with each subsequent read
so we don't loose data - or we simply ignore the slight possibility of two clients talking to
the server socket at the same moment. It MAY happen.

Alternatively we may simply spawn multiple pipes. This is theoretically also fine as long as
we remember which `pipe_name` was used already so each app gets their very own socket file
to use. This is probably the best approach in the long run. At least the one app "X4-External-App"
that I know of has a handy .env file where the PIPE variable can be set by it's user. It also checks
the received format and ignores data not suited for it.

At the moment we raise only one pipe server no matter how often this is requested.

We may even do both.

    …and also do UDP while we're on it.
--]]
function L.open_pipe(pipe_name)
    if(L.last_error == L.ERROR_MISSING_LIBRARY or L.last_error == L.ERROR_IO_DENIED) then
        -- no use in trying again
        return nil
    end

    -- We'll ignore filepath on Linux and use our own -- sorry
    local t_filename = explode(pipe_name, "\\")
    L.socket_filename = t_filename[#t_filename]
    t_filename = nil

    local socket_unix = require_socket_unix()

    if not socket_unix then
        log("Failed to load socket library")
        log(" * Is lua5.1-socket installed?")
        log(" * Did you symlink `/lib64/lua/5.1/socket` to `game/ui/core/lualibs/` or `sn_mod_support_apis/lua/c_library/` ?")
        log("Hint: `ln -s /lib64/lua/5.1/socket /path/to/X4_Foundations/game/ui/core/lualibs/`")
        log("WARN: lua5.4-socket does NOT work")
        CallEventScripts("directChatMessageReceived", "LinPipe: LuaSocket not found!\nlua5.1-socket must be installed\nand linked to game/ui/core/lualibs/\nor sn_mod_support_apis/lua/c_library/")
    end

    if (socket_unix and not L.server) then
        L.server = create_socket_server(L.socket_filename, socket_unix)
    end

    if(L.server and not L.pipe) then
        L.pipe = Pipe:create(L)
    end

    -- this returns a new instance of Pipe with write(message) close() read()
    return L.pipe
end

return L