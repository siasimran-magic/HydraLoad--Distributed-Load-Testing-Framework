import socket
import threading
import time
import random
import json

def handle_client(c, a):
    try:
        c.settimeout(5)
        c.recv(4096)
        # Simulate server processing time (10ms to 150ms)
        time.sleep(random.uniform(0.01, 0.15))
        
        # 5% chance to throw a 500 Error
        ok = random.random() > 0.05
        st = '200 OK' if ok else '500 Internal Server Error'
        
        b = json.dumps({'ok': ok}).encode()
        response = f'HTTP/1.1 {st}\r\nContent-Length: {len(b)}\r\nConnection: close\r\n\r\n'.encode() + b
        c.sendall(response)
    except Exception:
        pass
    finally:
        c.close()

s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(('0.0.0.0', 8080))
s.listen(128)

print("Dummy target running on http://127.0.0.1:8080")

while True:
    c, a = s.accept()
    threading.Thread(target=handle_client, args=(c, a), daemon=True).start()