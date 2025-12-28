import asyncio
import socket
import threading
import time  # noqa: F401

HOST = "localhost"
PORT = 6379
mem = {}

async def read_line(reader):
    # read until meet \r\n
    buffer = bytearray()
    while buffer[-2:] != b"\r\n":
        byte = await reader.readexactly(1)
        if not byte:
            return None
        buffer += byte
    return buffer[:-2]


async def parse_resp(reader):
    # Parse RESP Array, ví dụ:
    # *2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n
    # => ["GET", "foo"]
    first = await read_line(reader)
    if first is None:
        return None
    if first[0:1] != b"*":
        raise ValueError("Only RESP Array Supported")
    n = int(first[1:])
    ans = []
    for i in range(n):
        len_line = await read_line(reader)
        comment = await read_line(reader)
        ans.append(comment.decode())
    return ans


def is_expired(expire_time):
    if expire_time is None:
        return False
    return time.time() >= expire_time

async def handle_client(reader, writer):
    addr = writer.get_extra_info("peername")
    print(f"Client connected: {addr}")

    try:
        while True:
            command = await parse_resp(reader)
            if not command:
                break
            cmd = command[0].upper()
            # print(f"command: {command}, cmd = {cmd}, len cmd = {len(command)}")
            if cmd == "PING":
                writer.write(b"+Pong\r\n")
            elif cmd == "ECHO" and len(command) == 2:
                msg = command[1]
                resp = f"${len(msg)}\r\n{msg}\r\n".encode()
                # connection.sendall(resp)
                writer.write(resp)
            elif cmd == "SET":
                key = command[1]
                value = command[2]
                expire_at = None
                if len(command) >= 5:
                    opt = command[3].upper()
                    t = float(command[4])
                    sec = t if opt == "PX" else t / 1000
                    expire_at = time.time() + sec
                mem[key] = (value, expire_at)


                # connection.sendall(b"+OK\r\n")
                writer.write(b"+OK\r\n")
            elif cmd == "GET" and len(command) == 2:
                key = command[1]
                if key in mem:
                    value, expire_at = mem[key]
                    if is_expired(expire_at):
                        del mem[key]
                        writer.write(b"$-1\r\n")
                    else:
                        resp = f"${len(value)}\r\n{value}\r\n".encode()
                        # connection.sendall(resp)
                        writer.write(resp)
                else:
                    # connection.sendall(b"$-1\r\n") # null bulk string
                    writer.write(b"$-1\r\n")
            else:
                writer.write(b"-ERR unknown command\r\n")
    except asyncio.IncompleteReadError:
        pass
    finally:
        writer.close()
        await writer.wait_closed()
        print(f"Client disconnected: {addr}")
            

async def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment the code below to pass the first stage
    #
    # serverSocket = socket.create_server(("localhost", 6379))
    # serverSocket.listen()
    server = await asyncio.start_server(handle_client, HOST, PORT)
    print(f"Server listening on port 6379.")
    # connection.sendall(b"+PONG\r\n")

    # while True:
    #     connection, clientAddress = serverSocket.accept()
    #     clientThread = threading.Thread(
    #         target=handle_client,
    #         args=(connection, clientAddress),
    #         daemon=True
    #     )
    #     clientThread.start()
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
