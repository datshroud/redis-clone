import asyncio

from app.commands import handle_command
from app.protocol import RESPParser

HOST = "localhost"
PORT = 6379

async def handle_client(reader, writer):
    addr = writer.get_extra_info("peername")
    print(f"Client connected: {addr}")

    parser = RESPParser()

    try:
        while True:
            data = await reader.read(4096)
            if not data:
                break
            parser.feed(data)
            while True:
                cmd = parser.parse()
                if not cmd:
                    break
                resp = await handle_command(cmd)
                writer.write(resp)
                await writer.drain()
    except Exception as e:
        print("Error: ", e)
    finally:
        writer.close()
        await writer.wait_closed()
        print(f"Client disconnected: {addr}")

async def start_server():
    # You can use print statements as follows for debugging, they'll be 
    # visible when running tests.
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