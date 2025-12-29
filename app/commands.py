# =======================
# COMMAND HANDLER
# =======================

from csv import writer
import time

from app.storage import is_expired

kv_mem = {}
expire_mem = {}

def resp_simple(s) -> bytes:
    return f"+{s}\r\n".encode()
def resp_bulk(s: str | None) -> bytes:
    if s is None:
        return b"$-1\r\n"
    return f"${len(s)}\r\n{s}\r\n".encode()
def resp_int(n: int) -> bytes:
    return f":{n}\r\n".encode()
def resp_error(msg: int) -> bytes:
    return f"-{msg}\r\n".encode()

async def handle_command(command):
    if not command:
        return resp_error("empty command")
    
    name = command[0].upper()
    if name == "PING":
        return resp_simple("PONG")
    elif name == "ECHO" and len(command) == 2:
        return resp_bulk(command[1])
    elif name == "SET" and len(command) >= 3:
        key = command[1]
        val = command[2]
        if len(command) == 5:
            opt = command[3].upper()
            if opt != "EX" and opt != "PX":
                return resp_error(f"What is {opt}? We only have EX and PX.")
            t = float(command[4])
            sec = t if opt == "EX" else t / 1000
            expire_time = time.time() + sec
            expire_mem[key] = expire_time
        kv_mem[key] = val
        return resp_simple("OK")
    elif name == "GET" and len(command) == 2:
        key = command[1]
        if key not in kv_mem or is_expired(key):
            return resp_bulk(None)
        return resp_bulk(kv_mem[key])
    
    return resp_error("ERR unknown command")