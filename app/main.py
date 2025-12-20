import socket  # noqa: F401




def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment the code below to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379))
    connection, _ = server_socket.accept() # wait for client
    print(f"Client connected: {_}")
    # connection.sendall(b"+PONG\r\n")

    while True:
        data = connection.recv(1024)
        if not data:
            break
        connection.sendall(b"+PONG\r\n")
    connection.close()
    server_socket.close()

if __name__ == "__main__":
    main()
