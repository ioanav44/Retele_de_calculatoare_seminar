import socket

HOST        = '127.0.0.1'
PORT        = 9999
BUFFER_SIZE = 1024

clienti_conectati = {}

# mesaje: id -> {"autor": (ip, port), "text": str}
mesaje = {}
urmatorul_id = 1

server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
server_socket.bind((HOST, PORT))

print("=" * 50)
print(f"  SERVER UDP pornit pe {HOST}:{PORT}")
print("  Asteptam mesaje de la clienti...")
print("=" * 50)

while True:
    try:
        date_brute, adresa_client = server_socket.recvfrom(BUFFER_SIZE)
        mesaj_primit = date_brute.decode('utf-8').strip()

        parti = mesaj_primit.split(' ', 1)
        comanda = parti[0].upper()
        argumente = parti[1] if len(parti) > 1 else ''

        print(f"\n[PRIMIT] De la {adresa_client}: '{mesaj_primit}'")

        if comanda == 'CONNECT':
            if adresa_client in clienti_conectati:
                raspuns = "EROARE: Esti deja conectat la server."
            else:
                clienti_conectati[adresa_client] = True
                nr_clienti = len(clienti_conectati)
                raspuns = f"OK: Conectat cu succes. Clienti activi: {nr_clienti}"
                print(f"[SERVER] Client nou conectat: {adresa_client}")

        elif comanda == 'DISCONNECT':
            if adresa_client in clienti_conectati:
                del clienti_conectati[adresa_client]
                raspuns = "OK: Deconectat cu succes. La revedere!"
                print(f"[SERVER] Client deconectat: {adresa_client}")
            else:
                raspuns = "EROARE: Nu esti conectat la server."

        elif comanda == 'PUBLISH':
            if adresa_client not in clienti_conectati:
                raspuns = "EROARE: Nu esti conectat la server."
            else:
                text = argumente.strip()
                if not text:
                    raspuns = "EROARE: Mesajul pentru PUBLISH nu poate fi gol."
                else:
                    mesaj_id = urmatorul_id
                    urmatorul_id += 1
                    mesaje[mesaj_id] = {"autor": adresa_client, "text": text}
                    raspuns = f"OK: Mesaj publicat cu ID={mesaj_id}"

        elif comanda == 'DELETE':
            if adresa_client not in clienti_conectati:
                raspuns = "EROARE: Nu esti conectat la server."
            else:
                arg = argumente.strip()
                try:
                    mesaj_id = int(arg)
                except ValueError:
                    raspuns = "EROARE: ID invalid. Foloseste: DELETE <id_intreg>"
                else:
                    if mesaj_id not in mesaje:
                        raspuns = "EROARE: ID negasit."
                    else:
                        if mesaje[mesaj_id]["autor"] != adresa_client:
                            raspuns = "EROARE: Nu esti autorul acestui mesaj."
                        else:
                            del mesaje[mesaj_id]
                            raspuns = f"OK: Mesaj sters. ID={mesaj_id}"

        elif comanda == 'LIST':
            if adresa_client not in clienti_conectati:
                raspuns = "EROARE: Nu esti conectat la server."
            else:
                if not mesaje:
                    raspuns = "OK: Nu exista mesaje publicate."
                else:
                    linii = ["OK: Mesaje publicate:"]
                    for mesaj_id in sorted(mesaje.keys()):
                        linii.append(f"{mesaj_id}: {mesaje[mesaj_id]['text']}")
                    raspuns = "\n".join(linii)

        else:
            raspuns = f"EROARE: Comanda '{comanda}' este necunoscuta. Comenzi valide: CONNECT, DISCONNECT, PUBLISH, DELETE, LIST"

        server_socket.sendto(raspuns.encode('utf-8'), adresa_client)
        print(f"[TRIMIS]  Catre {adresa_client}: '{raspuns}'")

    except KeyboardInterrupt:
        print("\n[SERVER] Oprire server...")
        break
    except Exception as e:
        print(f"[EROARE] {e}")

server_socket.close()
print("[SERVER] Socket inchis.")
