from typing import Awaitable, Callable, Dict, List

from app.commands.list import (
    cmd_blpop, 
    cmd_llen, 
    cmd_lpop,
    cmd_lpush, 
    cmd_lrange, 
    cmd_rpush
)
from app.commands.misc import cmd_type
from app.commands.stream import cmd_xadd, cmd_xrange, cmd_xread
from app.commands.string import cmd_echo, cmd_get, cmd_incr, cmd_ping, cmd_set
from app.protocol.resp import resp_error


Handler = Callable[[List[str]], Awaitable[bytes]]

COMMAND_TABLE: Dict[str, Handler] = {
	"PING": cmd_ping,
	"ECHO": cmd_echo,
	"SET": cmd_set,
	"GET": cmd_get,
	"INCR": cmd_incr,
	"RPUSH": cmd_rpush,
	"LPUSH": cmd_lpush,
	"LRANGE": cmd_lrange,
	"LLEN": cmd_llen,
	"LPOP": cmd_lpop,
	"BLPOP": cmd_blpop,
	"TYPE": cmd_type,
	"XADD": cmd_xadd,
	"XRANGE": cmd_xrange,
	"XREAD": cmd_xread,
}

async def dispatch_command(command):
    if not command:
        return resp_error("ERR invalid command")
    name = command[0].upper()
    handler = COMMAND_TABLE.get(name)
    if handler is None:
        return resp_error("ERR unknown command")
    return await handler(command)