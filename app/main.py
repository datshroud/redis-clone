import socket
import threading  # noqa: F401

mem = {}

def parseResp(data: bytes):
    # Parse RESP Array, ví dụ:
    # *2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n
    # => ["GET", "foo"]
    arr = data.decode().split("\r\n")
    if arr[0][0] != '*':
        return []
    ans = []
    idx = 1
    while idx < len(arr):
        word = arr[idx]
        # print(word)
        if len(word) > 0 and word[0] == '$':
            ans.append(arr[idx + 1])
            idx += 2
        else:
            idx += 1
        # print(idx, ans)
    return ans


def handleClient(connection, clientAddress):
    print("Client connected, address:", clientAddress)

    try:
        while True:
            data = connection.recv(4096)
            if not data:
                print("Client disconnected, address:", clientAddress)
                break
            print(f"Raw data from {clientAddress}: {data}")
            command = parseResp(data)
            cmd = command[0].upper()
            # print(f"command: {command}, cmd = {cmd}, len cmd = {len(command)}")
            if cmd == "PING":
                connection.sendall(b"+Pong\r\n")
            elif cmd == "ECHO" and len(command) == 2:
                msg = command[1]
                resp = f"${len(msg)}\r\n{msg}\r\n".encode()
                connection.sendall(resp)
            elif cmd == "SET" and len(command) == 3:
                key = command[1]
                value = command[2]
                mem[key] = value
                connection.sendall(b"+OK\r\n")
            elif cmd == "GET" and len(command) == 2:
                key = command[1]
                if key in mem:
                    value = mem[key]
                    resp = f"${len(value)}\r\n{value}\r\n".encode()
                    connection.sendall(resp)
                else:
                    connection.sendall(b"$-1\r\n") # null bulk string
            else:
                connection.sendall(b"-ERR unknown command\r\n")
    except Exception as e:
        print(f"Error with {clientAddress}: {e}")
    finally:
        connection.close()
            

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment the code below to pass the first stage
    #
    serverSocket = socket.create_server(("localhost", 6379))
    serverSocket.listen()
    print(f"Server listening on port 6379.")
    # connection.sendall(b"+PONG\r\n")

    while True:
        connection, clientAddress = serverSocket.accept()
        clientThread = threading.Thread(
            target=handleClient,
            args=(connection, clientAddress),
            daemon=True
        )
        clientThread.start()

if __name__ == "__main__":
    main()
