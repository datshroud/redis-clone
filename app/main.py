import socket
import threading  # noqa: F401

def parseResp(data: bytes):
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
