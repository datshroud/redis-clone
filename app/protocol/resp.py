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

def convert_dict_to_list(mp):
    res = []
    for k, v in mp.items():
        if isinstance(v, dict):
            v = convert_dict_to_list(v)
        res.append(k)
        res.append(v)
    return res

def resp_nested_array(arr):
    if arr is None:
        return b"*-1\r\n"
    if not arr:
        return b"*0\r\n"
    res = f"*{len(arr)}\r\n".encode()
    for a in arr:
        if isinstance(a, dict):
            a = convert_dict_to_list(a)    
        if isinstance(a, list) or isinstance(a, tuple):
            res += resp_nested_array(a)
        else:
            res += f"${len(a)}\r\n{a}\r\n".encode()
    return res

def resp_stream(stream):
    res = []
    for id, data in stream:
        kv = []
        for k, v in data.items():
            kv.append(k)
            kv.append(v)
        res.append([id, kv])
    return resp_nested_array(res)

class RESPParser:
    def __init__(self):
        self.buffer = bytearray()
    
    def feed(self, data: bytes):
        self.buffer += data

    def parse(self):
        # Parse RESP Array, ví dụ:
        # *2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n
        # => ["GET", "foo"]
        if not self.buffer:
            return None
        
        if self.buffer[0:1] != b"*":
            raise ValueError("Only RESP Array supported")
        
        pos = self.buffer.find(b"\r\n")
        if pos == -1:
            return None
        
        buffer = self.buffer
        try:
            n = int(buffer[1: pos])
        except ValueError:
            raise ValueError("Invalid array length")

        idx = pos + 2
        ans = []
        for i in range(n):
            if idx >= len(buffer):
                return None
            if buffer[idx: idx + 1] != b"$":
                raise ValueError("Invalid bulk string")
            pos = buffer.find(b"\r\n", idx)
            if pos == -1:
                return None
            length = int(buffer[idx + 1 : pos])
            start = pos + 2
            end = start + length
            if end + 2 > len(buffer):
                return None
            ans.append(buffer[start:end].decode())
            idx = end + 2
        self.buffer = buffer[idx:]
        return ans

