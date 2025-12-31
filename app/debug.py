import datetime
from pathlib import Path


LOG_FILE = Path(__file__).parent / "debug.log"
def log(msg):
    ts = datetime.now().strftime("%H:%M:%S.%f")
    with LOG_FILE.open("a") as f:
        f.write(f"[{ts}] {msg}\n")