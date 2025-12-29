# =======================
# RESP INCREMENTAL PARSER
# =======================
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

