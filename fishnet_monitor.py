import docker
import re
import threading
import time
import requests
import psycopg2
import os
import json
import urllib.request  # Standard: Um Webseiten aufzurufen


# --- Konfiguration ---
# DB_HOST = os.environ.get('DB_HOST', 'postgres_db')
# DB_NAME = os.environ.get('DB_NAME', 'fishnet_stats')
# DB_USER = os.environ.get('DB_USER', '')
# DB_PASS = os.environ.get('DB_PASS', '')#.strip()
DB_HOST = 'postgres_db' 
DB_NAME = 'fishnet_stats'
DB_USER = ''
DB_PASS = '' # Direkt als String

# Regex für Lichess-Logs
LOG_PATTERN = re.compile(r"https://lichess\.org/(?P<id>[A-Za-z0-9_-]+).+finished\s+\((?P<knps>\d+)\s+knps/core\)")

def get_db_connection():
    while True:
        try:
            conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS, connect_timeout=5)
            print("Datenbank verbunden!")
            return conn
        except Exception as e:
            # Hier siehst du jetzt den echten Grund (z.B. "password authentication failed" oder "could not translate host name")
            print(f"Verbindungsfehler: {e}") 
            print("Warte auf SQL-Datenbank...")
            time.sleep(5)

conn = get_db_connection()
cur = conn.cursor()

# Tabelle mit ALLEN Feldern initialisieren
cur.execute("""
    CREATE TABLE IF NOT EXISTS metrics (
        id SERIAL PRIMARY KEY,
        time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        container_name TEXT, status VARCHAR(2), error_msg TEXT, 
        batch_id TEXT, analysis_type TEXT, variant TEXT, nps_si BIGINT, cores INTEGER, version TEXT,
        pbo_ppt_watt FLOAT, pbo_edc_ampere FLOAT, temp_c FLOAT,chess_com_players BIGINT,
        lichess_online INTEGER, lichess_games INTEGER,
        user_acquired INTEGER, user_queued INTEGER, user_oldest INTEGER,
        system_acquired INTEGER, system_queued INTEGER, system_oldest INTEGER
    );
""")
conn.commit()
# Tabelle für den Reset-Schutz und kumulative Stats
cur.execute("""
    CREATE TABLE IF NOT EXISTS worker_stats (
        id SERIAL PRIMARY KEY,
        time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        container_name TEXT,
        version TEXT,
        total_batches BIGINT,
        total_positions BIGINT,
        total_nodes BIGINT,
        cores INTEGER,
        platform_type TEXT
    );
""")
conn.commit()

client = docker.from_env()

# Globaler Cache für Lichess-Werte (Startwerte None)
cached_stats = {
    "players": None, "games": None, 
    "u_acq": None, "u_que": None, "u_old": None,
    "s_acq": None, "s_que": None, "s_old": None, "chess_com_players": None,
    "last_update": 0
}
monitored_containers = set()

def extract_json_data(text, pattern):
    match = re.search(pattern, text)
    return json.loads(match.group(0)) if match else None

# Chess.com Players

def fetch_chess_com_players():
    # Auf der Seite suchen
    url = "https://www.chess.com/play/online"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as response:
            html_text = response.read().decode('utf-8')
        
        # Regex:
        # window.chesscom.statsMembersOnline = 215862;
        js_match = re.search(r'statsMembersOnline\s*=\s*(\d+)', html_text)
        if js_match:
            return int(js_match.group(1))

        # Zweiter Versuch
        meta_match = re.search(r'(\d{1,3}(?:[.,]\d{3})+) users online', html_text, re.IGNORECASE)
        if meta_match:
            return int(''.join(filter(str.isdigit, meta_match.group(1))))
            
    except Exception as e:
        print(f"[!] Chess.com Scrape Fehler: {e}")
    return None

def update_lichess_stats_worker():
    global cached_stats
    # Vorgaukeln
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    while True:
        try:
            # 1. Hauptseite 
            # Chess.com Update
            new_chess_val = fetch_chess_com_players()
            if new_chess_val:
                cached_stats["chess_com_players"] = new_chess_val
                print(f"[OK] Chess.com Sync: {new_chess_val} Players")
            
            r1 = requests.get("https://lichess.org/", headers=headers, timeout=5)
            
            # Suche
            players_match = re.search(r'"members":(\d+)', r1.text)
            games_match = re.search(r'"rounds":(\d+)', r1.text)
            
            if players_match:
                cached_stats["players"] = int(players_match.group(1))
            if games_match:
                cached_stats["games"] = int(games_match.group(1))

            # 2. Fishnet Status (Queues)
            # Regex
            r2 = requests.get("https://lichess.org/fishnet/status", headers=headers, timeout=5)
            d2 = extract_json_data(r2.text, r'{"analysis":{"user":{.*?},"system":{.*?}}}')
            if d2:
                u, s = d2["analysis"]["user"], d2["analysis"]["system"]
                cached_stats.update({
                    "u_acq": u.get("acquired", None), "u_que": u.get("queued", None), "u_old": u.get("oldest", None),
                    "s_acq": s.get("acquired", None), "s_que": s.get("queued", None), "s_old": s.get("oldest", None)
                })
            
            cached_stats["last_update"] = time.time()
            
        except Exception as e:
            # Falls es doch mal hakt, siehst du es hier
            print(f"[!] Lichess Update Fehler (Python): {e}")
        
        time.sleep(60)# Update  60 

def get_pbo_data():
    stats = {"ppt": None, "edc": None, "temp": None}
    try:
        r = requests.get("http://host.docker.internal:8085/data.json", timeout=2)
        def find_nodes(node):
            t, v_raw = node.get('Text', ''), node.get('Value', '0')
            v = float(v_raw.split(' ')[0].replace(',', '.')) if ' ' in v_raw else None
            if t == "Package" and "/amdcpu/0/power" in node.get('SensorId', ''): stats["ppt"] = v
            # elif t == "TDC": stats["/amdcpu/0/current/0"] = v
            #elif node.get('SensorId') == "/amdcpu/0/current/0": stats["edc"] = v
            elif node.get('SensorId') == "/amdcpu/0/current/0" or t == "EDC": 
                stats["edc"] = v
            elif t == "Core (Tctl/Tdie)": stats["temp"] = v
            for child in node.get('Children', []): find_nodes(child)
        find_nodes(r.json())
        
    except: pass
    return stats

        
def stream_logs(container_name):
    try:
        container = client.containers.get(container_name)
        env = container.attrs.get('Config', {}).get('Env', [])
        cores = next((int(v.split('=')[1]) for v in env if v.startswith("CORES=")), 0)
        current_version = "unknown"
        try:
            version_exec = container.exec_run("/fishnet --version")
            if version_exec.exit_code != 0:
                version_exec = container.exec_run("fishnet --version")
            
            v_output = version_exec.output.decode().strip()
            v_match = re.search(r'(\d+\.\d+\.\d+)', v_output)
            current_version = v_match.group(1) if v_match else None
        except Exception:
            current_version = None
            
        for line in container.logs(stream=True, follow=True, tail=0):
            msg = line.decode('utf-8', errors='ignore').strip()
            if not msg: continue

            # --- HEADER ---
            if "total nodes" in msg and "><>" in msg:
                h_match = re.search(r'><>\s+v([\d\.]+):.*?([\d\.]+)\s+batches,\s+([\d\.]+)\s+positions,\s+([\d\.]+)\s+total nodes', msg)
                if h_match:
                    v_header = h_match.group(1)
                    b_total = int(h_match.group(2).replace('.', ''))
                    p_total = int(h_match.group(3).replace('.', ''))
                    n_total = int(h_match.group(4).replace('.', ''))
                    
                    try:
                        # Prüfen, ob Fishnet "abgekackt" ist (Wert kleiner als in DB)
                        cur.execute("""
                            SELECT total_nodes, total_batches, total_positions 
                            FROM worker_stats WHERE container_name = %s 
                            ORDER BY time DESC LIMIT 1
                        """, (container_name,))
                        row = cur.fetchone()
                        
                        if row and n_total < row[0]:
                            print(f"[!!!] RESET erkannt bei {container_name}: {n_total} < {row[0]}. Repariere...")
                            
                            # JSON-Backup nach deinem Format
                            stats_json = {
                                "total_batches": row[1],
                                "total_positions": row[2],
                                "total_nodes": row[0]
                            }
                            json_payload = json.dumps(stats_json).replace('"', '\\"')
                            
                            # Force-Write in die stats.file im Container
                            write_cmd = f"echo '{json_payload}' > /var/lib/fishnet/fishnet.stats"
                            container.exec_run(f"sh -c \"{write_cmd}\"")
                            
                            # Neustart erzwingen
                            container.restart()
                            print(f"[#] {container_name} mit alten Werten neu gestartet.")
                            return # Thread beenden, main() übernimmt nach Restart

                        # Normaler Insert
                        cur.execute("""
                            INSERT INTO worker_stats (
                                time, container_name, version, 
                                total_batches, total_positions, total_nodes, cores, 
                                platform_type
                            ) VALUES (CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s, 'DOCKER')
                            ON CONFLICT DO NOTHING
                        """, (container_name, v_header, b_total, p_total, n_total, cores))
                        conn.commit()
                        print(f"[{container_name}] HEADER: {b_total} Batches | {p_total} Pos | {n_total} Nodes")
                    except Exception as e:
                        conn.rollback()
                        print(f"Fehler im Header-Prozess ({container_name}): {e}")

            # Initialisierung Fallback 
            status, error_msg, nps_si, batch_id = None, None, None, None
            analysis_type, variant = None, None

            # Fehler
            if msg.startswith(("W:", "E:")):
                parts = msg.split(":", 1)
                status = parts[0].strip()
                error_msg = parts[1].strip()

            # fishnet finished
            elif "finished" in msg:
                status = "OK"
                id_match = re.search(r'(?:https://lichess\.org/(\w+)|batch (\w+))', msg)
                if id_match:
                    batch_id = id_match.group(1) or id_match.group(2)
                    analysis_type = "USER" if "https" in msg else "SYSTEM"
                
                # Variante NPS
                # Erkennt optional die Variante und zwingend die knps/core
                #variant_match = re.search(r'\((?:([\w\d]+),\s+)?(\d+)\s+knps/core\)', msg)
                #variant_match = re.search(r'\((?:([a-zA-Z]+),\s+)?(?:[\w\d]+,\s+)?(\d+)\s+knps/core\)', msg)
                variant_match = re.search(r'\((?:([\w\d]+),\s+)?(?:[\w\d]+,\s+)?(\d+)\s+knps/core\)', msg)
                
                if variant_match:
                    # Falls Gruppe 1 leer nur knps im Log), setze "standard"
                    variant = variant_match.group(1) if variant_match.group(1) else "standard"
                    # Umrechnung in nps
                    nps_si = int(variant_match.group(2)) * 1000
                else:
                    # Fallback
                    variant = None
                    
            # Speichern (OK, W , E)
            if status:
                pbo = get_pbo_data()
                l = cached_stats
                try:
                    cur.execute("""
                        INSERT INTO metrics (
                            container_name, status, error_msg, batch_id, analysis_type, variant, nps_si, cores, version, 
                            pbo_ppt_watt, pbo_edc_ampere, temp_c, chess_com_players, lichess_online, lichess_games,
                            user_acquired, user_queued, user_oldest, system_acquired, system_queued, system_oldest
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        container_name, status, error_msg, batch_id, analysis_type, variant, nps_si, cores, current_version,
                        pbo['ppt'], pbo['edc'], pbo['temp'], l['chess_com_players'],  l['players'], l['games'],
                        l['u_acq'], l['u_que'], l['u_old'], l['s_acq'], l['s_que'], l['s_old']
                    ))
                    conn.commit()
                except Exception as db_err:
                    conn.rollback()
                
                print(f"[{container_name}] ID:{batch_id} | {analysis_type} | {status} | {error_msg} | {variant} | {nps_si}nps | {cores}C | {current_version} | {pbo['temp']}C | {pbo['ppt']}W | {pbo['edc']}A | ChessCom: {cached_stats['chess_com_players']}u | L-Stat:{cached_stats['players']}u/{cached_stats['games']}g | User(A/Q/O):{cached_stats['u_acq']}/{cached_stats['u_que']}/{cached_stats['u_old']}s | Sys(A/Q/O):{cached_stats['s_acq']}/{cached_stats['s_que']}/{cached_stats['s_old']}s")
                
                #print(f"[{container_name}] ID:{batch_id} | {analysis_type} | {status} | {variant} | {nps_si}nps | {cores}C | {pbo['temp']}C | {pbo['ppt']}W | {pbo['edc']}A | User(A/Q/O):{cached_stats['u_acq']}/{cached_stats['u_que']}/{cached_stats['u_old']}s")

    except Exception as e:
        print(f"Fehler in stream_logs für {container_name}: {e}")
    finally:
        monitored_containers.discard(container_name)

def main():
    # Lichess-Hintergrund-Thread
    threading.Thread(target=update_lichess_stats_worker, daemon=True).start()
    
    print("Starte SI-Logger mit Fishnet-Queue-Analyse...")
    while True:
        for c in client.containers.list():
            if "fishnet-" in c.name and c.name not in monitored_containers:
                monitored_containers.add(c.name)
                threading.Thread(target=stream_logs, args=(c.name,), daemon=True).start()
        time.sleep(5)

if __name__ == "__main__":
    main()
