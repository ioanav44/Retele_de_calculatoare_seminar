import socket
import threading

HOST = "127.0.0.1"
PORT = 3333
BUFFER_SIZE = 1024

class State:
    def __init__(self):
        self.data = {}
        self.lock = threading.Lock()

    def add(self, key, value):
        with self.lock:
            self.data[key] = value
        return f"{key} added"

    def get(self, key):
        with self.lock:
            return self.data.get(key, "Key not found")

    def remove(self, key):
        with self.lock:
            if key in self.data:
                del self.data[key]
                return f"{key} removed"
            return "Key not found"

    def list_items(self):
        with self.lock:
            items = [f"{k}={v}" for k, v in self.data.items()]
        return "DATA|" + ",".join(items)

    def count(self):
        with self.lock:
            count = len(self.data)
        return f"DATA {count}"

    def clear(self):
        with self.lock:
            self.data.clear()
        return "all data deleted"

    def update(self, key, value):
        with self.lock:
            if key not in self.data:
                return "Key not found"
            self.data[key] = value
        return "Data updated"

    def pop(self, key):
        with self.lock:
            if key not in self.data:
                return "Key not found"
            value = self.data.pop(key)
        return f"DATA {value}"

state = State()

def process_command(command):
    parts = command.split()
    if not parts:
        return "Invalid command format"

    cmd = parts[0].lower()

    if cmd == "add":
        if len(parts) < 3:
            return "Invalid command format"
        key = parts[1]
        value = ' '.join(parts[2:])
        return state.add(key, value)
    elif cmd == "get":
        if len(parts) != 2:
            return "Invalid command format"
        return state.get(parts[1])
    elif cmd == "remove":
        if len(parts) != 2:
            return "Invalid command format"
        return state.remove(parts[1])
    elif cmd == "list":
        if len(parts) != 1:
            return "Invalid command format"
        return state.list_items()
    elif cmd == "count":
        if len(parts) != 1:
            return "Invalid command format"
        return state.count()
    elif cmd == "clear":
        if len(parts) != 1:
            return "Invalid command format"
        return state.clear()
    elif cmd == "update":
        if len(parts) < 3:
            return "Invalid command format"
        key = parts[1]
        value = ' '.join(parts[2:])
        return state.update(key, value)
    elif cmd == "pop":
        if len(parts) != 2:
            return "Invalid command format"
        return state.pop(parts[1])
    elif cmd in {"quit", "exit"}:
        return "__QUIT__"

    return "Invalid command"

def handle_client(client_socket):
    with client_socket:
        while True:
            try:
                data = client_socket.recv(BUFFER_SIZE)
                if not data:
                    break

                command = data.decode('utf-8').strip()
                response = process_command(command)

                if response == "__QUIT__":
                    response = "OK bye"
                    response_data = f"{len(response)} {response}".encode('utf-8')
                    client_socket.sendall(response_data)
                    break
                
                response_data = f"{len(response)} {response}".encode('utf-8')
                client_socket.sendall(response_data)

            except Exception as e:
                err = f"Error: {str(e)}"
                client_socket.sendall(f"{len(err)} {err}".encode('utf-8'))
                break

def start_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((HOST, PORT))
        server_socket.listen()
        print(f"[SERVER] Listening on {HOST}:{PORT}")

        while True:
            client_socket, addr = server_socket.accept()
            print(f"[SERVER] Connection from {addr}")
            threading.Thread(target=handle_client, args=(client_socket,)).start()

if __name__ == "__main__":
    start_server()
