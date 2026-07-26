import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

import os
import re
import glob
import json
import threading
import subprocess
import webbrowser
import duckdb
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
import uvicorn

app = FastAPI()
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Connect to in-memory DuckDB for fast querying of parquets
con = duckdb.connect(database=':memory:')

task_state = {
    'status': 'idle',
    'pct': 0,
    'msg': '',
    'error': None
}

def scan_datasets():
    datasets = []
    # Scan for Parquet instead of CSV
    pattern = os.path.join(SCRIPT_DIR, "*_aggtrades_*.parquet")
    
    for filepath in sorted(glob.glob(pattern)):
        basename = os.path.basename(filepath)
        
        if "_spot_aggtrades_" in basename:
            source = "spot"
        elif "_futures_aggtrades_" in basename:
            source = "futures"
        else:
            continue
        
        parts = basename.split(f"_{source}_aggtrades_")
        if len(parts) != 2:
            continue
            
        symbol = parts[0].upper()
        time_part = parts[1].replace(".parquet", "")
        
        m_new = re.match(r"(\d{8})_(\d{4})_(\d{8})_(\d{4})", time_part)
        
        if m_new:
            start_str = f"{m_new.group(1)[:4]}-{m_new.group(1)[4:6]}-{m_new.group(1)[6:8]} {m_new.group(2)[:2]}:{m_new.group(2)[2:]} UTC"
            end_str = f"{m_new.group(3)[:4]}-{m_new.group(3)[4:6]}-{m_new.group(3)[6:8]} {m_new.group(4)[:2]}:{m_new.group(4)[2:]} UTC"
        else:
            start_str = time_part
            end_str = ""
        
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        
        datasets.append({
            'filename': basename,
            'symbol': symbol,
            'source': source,
            'start_display': start_str,
            'end_display': end_str,
            'size_mb': round(size_mb, 2),
            'time_suffix': time_part
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
            
            if "Istek #" in line:
                try:
                    parts = line.split("|")
                    if "%" in line:
                        pct_str = parts[-1].strip().replace("%", "")
                        task_state['pct'] = float(pct_str)
                    else:
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


@app.get("/")
def get_dashboard():
    with open(os.path.join(SCRIPT_DIR, "dashboard.html"), "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

@app.get("/api/progress")
def get_progress():
    return JSONResponse(task_state)

@app.get("/api/datasets")
def get_datasets():
    return JSONResponse(scan_datasets())

@app.get("/replay")
def get_replay(file: str = "", symbol: str = "", source: str = "futures", time_suffix: str = ""):
    template_path = os.path.join(SCRIPT_DIR, "replay_template.html")
    with open(template_path, "r", encoding="utf-8") as f:
        html = f.read()
    
    html = html.replace("{{ symbol }}", symbol)
    html = html.replace("{{ source_title }}", "Spot" if source.lower() == "spot" else "Futures")
    html = html.replace("{{ trades_file }}", file)
    html = html.replace("{{ time_suffix }}", time_suffix)
    return HTMLResponse(html)


@app.get("/api/trades")
def api_trades(file: str, offset: int = 0, limit: int = 10000):
    filepath = os.path.join(SCRIPT_DIR, file)
    if not os.path.exists(filepath):
        return JSONResponse({"error": "File not found"}, status_code=404)
    
    query = f"SELECT p, q, T_ms as t, m FROM read_parquet('{filepath}') ORDER BY T_ms LIMIT {limit} OFFSET {offset}"
    df = con.execute(query).df()
    
    rows = []
    for row in df.itertuples(index=False):
        rows.append([round(row.p, 6), round(row.q, 4), int(row.t), bool(row.m)])
        
    return JSONResponse(rows)

@app.get("/api/liquidations")
def api_liquidations(symbol: str, time_suffix: str):
    file = f"{symbol.lower()}_futures_liquidations_{time_suffix}.parquet"
    filepath = os.path.join(SCRIPT_DIR, file)
    if not os.path.exists(filepath):
        return JSONResponse([])
        
    query = f"SELECT time, side, price, executedQty FROM read_parquet('{filepath}') ORDER BY time"
    df = con.execute(query).df()
    return JSONResponse(df.to_dict(orient="records"))


class FetchRequest(BaseModel):
    symbol: str = "XRPUSDT"
    source: str = "futures"
    start_time: str = "2025-10-10T20:00:00"
    end_time: str = "2025-10-10T23:59:59"

@app.post("/api/fetch")
def api_fetch(req: FetchRequest):
    threading.Thread(
        target=run_fetch_task,
        args=(req.symbol, req.source, req.start_time, req.end_time),
        daemon=True
    ).start()
    return JSONResponse({'status': 'started'})

if __name__ == "__main__":
    port = 8080
    url = f"http://localhost:{port}"
    print("\n==============================================")
    print(" Unified Crypto Workspace Dashboard (FastAPI) Baslatildi!")
    print(f" Panel Linki: {url}")
    print("==============================================\n")
    print("Tarayici otomatik olarak aciliyor...")
    webbrowser.open(url)
    
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="error")
