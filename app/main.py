import asyncio
from app.server import start_server  # noqa: F401

if __name__ == "__main__":
    asyncio.run(start_server())
