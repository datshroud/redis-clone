# =======================
# COMMAND HANDLER
# =======================

from csv import writer
import time

from app.storage import kv_mem, expire_mem, list_mem, is_expired


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

def resp_array(arr = None):
    if not arr:
        return b"*0\r\n"
    ans = f"*{len(arr)}\r\n".encode()
    for a in arr:
        ans += f"${len(a)}\r\n{a}\r\n".encode()
    return ans


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
            t = int(command[4])
            sec = t if opt == "EX" else t / 1000
            expire_time = time.time() + sec
            expire_mem[key] = expire_time
        else:
            expire_mem.pop(key, None)
        kv_mem[key] = val
        return resp_simple("OK")
    elif name == "GET" and len(command) == 2:
        key = command[1]
        if key not in kv_mem or is_expired(key):
            return resp_bulk(None)
        return resp_bulk(kv_mem[key])
    elif name == "RPUSH" and len(command) >= 3:
        key = command[1]
        if key in kv_mem:
            return resp_error("ERR int cannot append like list")
        if key not in list_mem:
            list_mem[key] = []
        for i in range(2, len(command)):
            list_mem[key].append(command[i])
        return resp_int(len(list_mem[key]))
    elif name == "LRANGE" and len(command) == 4:
        key = command[1]
        if key in kv_mem:
            return resp_error("ERR int cannot range like list")
        if key not in list_mem:
            return resp_array()
        arr = list_mem[key]
        try:
            narr = len(arr)
            start = int(command[2])
            end = int(command[3])
            if start < 0:
                start += narr
            if end < 0:
                end += narr + 1
            start = max(0, min(narr, start))
            end = max(0, min(end, narr))
        except ValueError:
            return resp_error("ERR value is not an integer")
        return resp_array(arr[start : end])
    elif name == "LPUSH" and len(command) >= 3:
        arr = command[2:]
        key = command[1]
        if key not in list_mem:
            list_mem[key] = []
        list_mem[key] = arr[::-1] + list_mem[key]
        return resp_int(len(list_mem[key]))
    elif name == "LLEN" and len(command) == 2:
        key = command[1]
        if key not in list_mem:
            return resp_int(0)
        return resp_int(len(list_mem[key]))
    elif name == "LPOP" and 2 <= len(command) <= 3:
        key = command[1]
        start = 1
        if len(command) == 3:
            try:
                start = max(0, int(command[2]))
            except ValueError:
                return resp_error("ERR value is not an integer")
        if key not in list_mem:
            return resp_bulk(None)
        pop_arr = list_mem[key][:start]
        list_mem[key] = list_mem[key][start:]
        return resp_array(pop_arr)
    return resp_error("ERR unknown command")

    