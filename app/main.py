import socket
import threading  # noqa: F401


def handleClient(connection, clientAddress):
    print("Client connected, address:", clientAddress)

    try:
        while True:
            data = connection.recv(1024)
            if not data:
                print("Client disconnected, address:", clientAddress)
                break
            print(f"Received from {clientAddress}: {data}")
            connection.sendall(b"+PONG\r\n")
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
