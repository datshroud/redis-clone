import asyncio
from csv import writer
import time

import app.storage as storage

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
    if arr is None:
        return b"*-1\r\n"
    if not arr:
        return b"*0\r\n"
    ans = f"*{len(arr)}\r\n".encode()
    for a in arr:
        ans += f"${len(a)}\r\n{a}\r\n".encode()
    return ans

def current_time_ms():
    return int(time.time() * 1000)

# =======================
# STREAM ID HANDLER
# =======================

def parse_stream_id(id):
    if id == "*":
        return None, None
    if '-' not in id:
        return None
    ms, seq = id.split('-', 1)
    if seq == "*":
        try:
            return int(ms), None
        except ValueError:
            return None
    try:
        return int(ms), int(seq)
    except ValueError:
        return None
    
def validate_stream_id(key, id):
    ms, seq = id
    if ms == 0 and seq == 0:
        return "ERR the ID specified in XADD must be greater than 0-0"
    stream = storage.stream_mem[key]
    if not stream:
        return None
    last_id, _ = stream[-1]
    last_ms, last_seq = parse_stream_id(last_id)

    if ms < last_ms:
        return "ERR The ID specified in XADD is equal or smaller than the target stream top item"
    if ms == last_ms and seq <= last_seq:
        return "ERR The ID specified in XADD is equal or smaller than the target stream top item"
    return None

def generate_sequence(key, ms):
    stream = storage.stream_mem.setdefault(key, [])
    if not stream:
        return 1 if ms == 0 else 0
    last_id, _ = stream[-1]
    last_ms, last_seq = parse_stream_id(last_id)
    return last_seq + 1 if last_ms == ms else 0

def generate_full_id(key):
    ms = current_time_ms()
    stream = storage.stream_mem.setdefault(key, [])
    if not stream:
        return ms, (0 if ms > 0 else 1)
    last_id, _ = storage.stream_mem[key][-1]
    last_ms, last_seq = parse_stream_id(last_id)
    return ms, (last_seq + 1 if last_ms == ms else 0)

# =======================
# COMMAND HANDLER
# =======================

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
            storage.expire_mem[key] = expire_time
        else:
            storage.expire_mem.pop(key, None)
        storage.kv_mem[key] = val
        return resp_simple("OK")
    elif name == "GET" and len(command) == 2:
        key = command[1]
        if key not in storage.kv_mem or storage.is_expired(key):
            return resp_bulk(None)
        return resp_bulk(storage.kv_mem[key])
    elif name == "RPUSH" and len(command) >= 3:
        key = command[1]
        if key in storage.kv_mem:
            return resp_error("ERR int cannot append like list")
        if key not in storage.list_mem:
            storage.list_mem[key] = []
        storage.list_mem[key] += command[2:]
        storage.notify_list_push(key)
        return resp_int(len(storage.list_mem[key]))
    elif name == "LRANGE" and len(command) == 4:
        key = command[1]
        if key in storage.kv_mem:
            return resp_error("ERR int cannot range like list")
        if key not in storage.list_mem:
            return resp_array()
        arr = storage.list_mem[key]
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
        if key not in storage.list_mem:
            storage.list_mem[key] = []
        storage.list_mem[key] = arr[::-1] + storage.list_mem[key]
        for i in range(2, len(command)):
            storage.notify_list_push(key)
        return resp_int(len(storage.list_mem[key]))
    elif name == "LLEN" and len(command) == 2:
        key = command[1]
        if key not in storage.list_mem:
            return resp_int(0)
        return resp_int(len(storage.list_mem[key]))
    elif name == "LPOP" and 2 <= len(command) <= 3:
        key = command[1]
        start = 1
        if len(command) == 3:
            try:
                start = max(0, int(command[2]))
            except ValueError:
                return resp_error("ERR value is not an integer")
        if key not in storage.list_mem:
            return resp_bulk(None)
        pop_arr = storage.list_mem[key][:start]
        storage.list_mem[key] = storage.list_mem[key][start:]
        return resp_array(pop_arr)
    elif name == "BLPOP" and len(command) == 3:
        key = command[1]
        try:
            timeout = float(command[2])
        except ValueError:
            return resp_error("ERR value is not a float")
        
        if key in storage.list_mem and storage.list_mem[key]:
            val = storage.list_mem[key].pop(0)
            return resp_array([key, val])
        
        fut = asyncio.get_event_loop().create_future()
        storage.waiter_mem.setdefault(key, []).append(fut)

        try:
            if timeout > 0:
                await asyncio.wait_for(fut, timeout)
            else:
                await fut
        except asyncio.TimeoutError:
            storage.waiter_mem[key].remove(fut)
            return resp_array()
        
        if key in storage.list_mem and storage.list_mem[key]:
            return resp_array([key, storage.list_mem[key].pop(0)])
        return resp_array()
    elif name == "TYPE" and len(command) == 2:
        key = command[1]
        if key in storage.kv_mem and not storage.is_expired(key):
            return resp_simple("string")
        elif key in storage.list_mem:
            return resp_simple("list")
        elif key in storage.stream_mem:
            return resp_simple("stream")
        return resp_simple("none")
    elif name == "XADD" and len(command) >= 5:
        # <milliseconds>-<sequence>
        key = command[1]
        id = command[2]
        if len(command) % 2 == 0:
            return resp_error("ERR wrong number of args for XADD")
        parsed = parse_stream_id(id)
        if not parsed:
            return resp_error("ERR invalid stream ID")
        if key in storage.kv_mem or key in storage.list_mem:
            return resp_error("ERR wrong type")
        if key not in storage.stream_mem:
            storage.stream_mem[key] = []
        ms, seq = parsed
        if ms is None and seq is None:
            ms, seq = generate_full_id(key)
        elif seq is None:
            seq = generate_sequence(key, ms)
        err = validate_stream_id(key, (ms, seq))
        if err:
            return resp_error(err)
        # print("ms, seq = ", ms, seq)
        id = f"{ms}-{seq}"
        data = {}
        for i in range(3, len(command), 2):
            data[command[i]] = command[i + 1]
        storage.stream_mem[key].append((id, data))
        return resp_bulk(id)
    return resp_error("ERR unknown command")

    