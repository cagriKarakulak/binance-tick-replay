"""
Unified Crypto Workspace Dashboard Server
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

import os
import re
import glob
import gzip
import json
import subprocess
import threading
import webbrowser
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Global State for background tasks
task_state = {
    'status': 'idle',  # idle, running, finished
    'pct': 0,
    'msg': '',
    'error': None
}

# In-memory cache: filename -> gzipped JSON bytes
_trades_cache = {}


def scan_datasets():
    """Scan the project directory for downloaded aggtrades CSV files and return metadata."""
    datasets = []
    pattern = os.path.join(SCRIPT_DIR, "*_aggtrades_*.csv")
    
    for filepath in sorted(glob.glob(pattern)):
        basename = os.path.basename(filepath)
        # Parse filename: e.g. atomusdt_futures_aggtrades_20251010_2000_20251010_2359.csv
        #                  or: atomusdt_futures_aggtrades_20251010.csv (old format)
        
        # Determine source (spot or futures)
        if "_spot_aggtrades_" in basename:
            source = "spot"
        elif "_futures_aggtrades_" in basename:
            source = "futures"
        else:
            continue
        
        # Extract symbol
        parts = basename.split(f"_{source}_aggtrades_")
        if len(parts) != 2:
            continue
        symbol = parts[0].upper()
        time_part = parts[1].replace(".csv", "")
        
        # Parse time info for display
        # New format: 20251010_2000_20251010_2359
        # Old format: 20251010
        m_new = re.match(r"(\d{8})_(\d{4})_(\d{8})_(\d{4})", time_part)
        m_old = re.match(r"(\d{8})$", time_part)
        
        if m_new:
            start_str = f"{m_new.group(1)[:4]}-{m_new.group(1)[4:6]}-{m_new.group(1)[6:8]} {m_new.group(2)[:2]}:{m_new.group(2)[2:]} UTC"
            end_str = f"{m_new.group(3)[:4]}-{m_new.group(3)[4:6]}-{m_new.group(3)[6:8]} {m_new.group(4)[:2]}:{m_new.group(4)[2:]} UTC"
        elif m_old:
            d = m_old.group(1)
            start_str = f"{d[:4]}-{d[4:6]}-{d[6:8]} (tum gun)"
            end_str = ""
        else:
            start_str = time_part
            end_str = ""
        
        # Get file size
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        
        datasets.append({
            'filename': basename,
            'symbol': symbol,
            'source': source,
            'start_display': start_str,
            'end_display': end_str,
            'size_mb': round(size_mb, 2)
        })
    
    return datasets


def run_fetch_task(symbol, source, start_time, end_time):
    global task_state
    task_state['status'] = 'running'
    task_state['pct'] = 0
    task_state['msg'] = 'Baslatiliyor...'
    task_state['error'] = None

    script_name = "fetch_spot_aggtrades.py" if source == "spot" else "fetch_futures_aggtrades.py"
    script_path = os.path.join(SCRIPT_DIR, script_name)
    
    cmd = [sys.executable, "-u", script_path, "--symbol", symbol,
           "--start", start_time, "--end", end_time]
    
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8'
        )

        for line in process.stdout:
            line = line.strip()
            if not line:
                continue
            
            # Parse progress: "Istek #  20 | Toplam Islem:   20,000 | Son Zaman: 21:13:02 UTC"
            if "Istek #" in line:
                try:
                    parts = line.split("|")
                    if "%" in line:
                        pct_str = parts[-1].strip().replace("%", "")
                        task_state['pct'] = float(pct_str)
                    else:
                        # Fake progress loop 10 -> 90 to show it's alive
                        current_pct = task_state['pct']
                        task_state['pct'] = current_pct + 2 if current_pct < 90 else 10
                        
                    task_state['msg'] = parts[1].strip() + " - " + parts[2].strip()
                except Exception:
                    task_state['msg'] = line
            else:
                if "HATA" in line or "Error" in line:
                    task_state['msg'] = line
                elif "Kaydedildi" in line or "OHLCV" in line:
                    task_state['msg'] = line
                    task_state['pct'] = 99

        process.wait()
        
        if process.returncode != 0:
            task_state['error'] = f"Islem basarisiz oldu. (Kod: {process.returncode})"
            
    except Exception as e:
        task_state['error'] = str(e)
    
    task_state['status'] = 'finished'
    task_state['pct'] = 100


def prepare_replay_data(filename):
    """Load a specific aggtrades CSV and return gzip-compressed JSON for the replay player.
    Results are cached in memory so subsequent loads are instant."""
    if filename in _trades_cache:
        return _trades_cache[filename]

    csv_path = os.path.join(SCRIPT_DIR, filename)
    
    if not os.path.exists(csv_path):
        return None

    print(f"[Cache] {filename} ilk kez yukleniyor...")
    df = pd.read_csv(csv_path)
    timestamps_ms = pd.to_datetime(df["T_str"], format='mixed', utc=True).astype('int64') // 10**6
    
    # Build compact list and serialize
    p = df['p'].astype(float).values
    q = df['q'].astype(float).values
    t = timestamps_ms.values
    m = df['m'].astype(bool).values
    
    # Build list with round() to reduce JSON size
    rows = []
    for i in range(len(p)):
        rows.append([round(float(p[i]), 8), round(float(q[i]), 4), int(t[i]), bool(m[i])])
    
    raw_json = json.dumps(rows).encode('utf-8')
    compressed = gzip.compress(raw_json, compresslevel=4)
    print(f"[Cache] {filename} yuklendi. Ham: {len(raw_json)//1024}KB -> Gzip: {len(compressed)//1024}KB")
    
    _trades_cache[filename] = compressed
    return compressed





class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        qs = urllib.parse.parse_qs(parsed.query)
        
        if path == '/':
            self.send_response(200)
            self.send_header('Content-type', 'text/html; charset=utf-8')
            self.end_headers()
            with open(os.path.join(SCRIPT_DIR, "dashboard.html"), "rb") as f:
                self.wfile.write(f.read())
                
        elif path == '/api/progress':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(task_state).encode('utf-8'))
        
        elif path == '/api/datasets':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            datasets = scan_datasets()
            self.wfile.write(json.dumps(datasets).encode('utf-8'))
            
        elif path == '/replay':
            filename = qs.get('file', [None])[0]
            symbol = qs.get('symbol', [''])[0].upper()
            source = qs.get('source', ['futures'])[0].lower()
            
            self.send_response(200)
            self.send_header('Content-type', 'text/html; charset=utf-8')
            self.end_headers()
            
            template_path = os.path.join(SCRIPT_DIR, "replay_template.html")
            with open(template_path, "r", encoding="utf-8") as f:
                html = f.read()
            
            html = html.replace("{{ symbol }}", symbol)
            html = html.replace("{{ source_title }}", "Spot" if source == "spot" else "Futures")
            # Inject the filename so JS can fetch /api/trades?file=...
            html = html.replace("{{ trades_file }}", filename or "")
            self.wfile.write(html.encode('utf-8'))
            
        elif path == '/api/trades':
            filename = qs.get('file', [None])[0]
            if not filename:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b'{"error": "file parameter required"}')
                return
                
            data = prepare_replay_data(filename)
            if data:
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.send_header('Content-Encoding', 'gzip')
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            else:
                self.send_response(404)
                self.end_headers()

        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not Found")

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == '/api/fetch':
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data)
            
            symbol = data.get('symbol', 'XRPUSDT')
            source = data.get('source', 'futures')
            start_time = data.get('start_time', '2025-10-10T20:00:00')
            end_time = data.get('end_time', '2025-10-10T23:59:59')
            
            # Start background thread
            threading.Thread(
                target=run_fetch_task,
                args=(symbol, source, start_time, end_time),
                daemon=True
            ).start()
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'status': 'started'}).encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass


if __name__ == "__main__":
    port = 8080
    server_address = ('', port)
    httpd = HTTPServer(server_address, DashboardHandler)
    
    url = f"http://localhost:{port}"
    print("\n==============================================")
    print(" Unified Crypto Workspace Dashboard Baslatildi!")
    print(f" Panel Linki: {url}")
    print("==============================================\n")
    print("Tarayici otomatik olarak aciliyor...")
    
    webbrowser.open(url)
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nKapatiliyor...")
        httpd.server_close()
