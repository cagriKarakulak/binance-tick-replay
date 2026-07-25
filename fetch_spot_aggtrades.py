"""
Binance Spot XRPUSDT - AggTrades verisi cekip 1s OHLCV mumlara donusturme
Tarih: 10 Ekim 2025, 20:00 - 23:59 UTC
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

import requests
import pandas as pd
import json
import time
import os
import concurrent.futures
import threading
from datetime import datetime, timezone

import argparse

# Global state for multithreading
progress_lock = threading.Lock()
total_requests = 0
fetched_trades_count = 0

# ============================================================
# AYARLAR
# ============================================================
parser = argparse.ArgumentParser()
parser.add_argument("--symbol", type=str, default="XRPUSDT", help="Islem paritesi (orn: BTCUSDT)")
parser.add_argument("--start", type=str, default="2025-10-10T20:00:00", help="Baslangic zamani (ISO format)")
parser.add_argument("--end", type=str, default="2025-10-10T23:59:59", help="Bitis zamani (ISO format)")
args, unknown = parser.parse_known_args()

SYMBOL = args.symbol.upper()
SYMBOL_LOWER = SYMBOL.lower()

# Parse the datetime from strings
START_DT = pd.to_datetime(args.start).replace(tzinfo=timezone.utc)
END_DT = pd.to_datetime(args.end).replace(tzinfo=timezone.utc)

START_MS = int(START_DT.timestamp() * 1000)
END_MS = int(END_DT.timestamp() * 1000)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Generate unique filenames based on the timestamps
time_suffix = f"{START_DT.strftime('%Y%m%d_%H%M')}_{END_DT.strftime('%Y%m%d_%H%M')}"
RAW_TRADES_CSV = os.path.join(SCRIPT_DIR, f"{SYMBOL_LOWER}_spot_aggtrades_{time_suffix}.csv")
SPOT_1S_CSV = os.path.join(SCRIPT_DIR, f"{SYMBOL_LOWER}_spot_1s_{time_suffix}.csv")


def fetch_chunk(symbol, chunk_start, chunk_end, chunk_id, total_chunks):
    global total_requests, fetched_trades_count
    url = "https://api.binance.com/api/v3/aggTrades"
    chunk_trades = []
    current_start = chunk_start
    retry_count = 0
    
    while current_start < chunk_end:
        params = {
            "symbol": symbol,
            "startTime": current_start,
            "endTime": chunk_end,
            "limit": 1000
        }

        try:
            resp = requests.get(url, params=params, timeout=30)
        except requests.exceptions.RequestException as e:
            retry_count += 1
            if retry_count > 5:
                with progress_lock:
                    print(f"  [Parca {chunk_id}/{total_chunks}] HATA: 5 deneme basarisiz: {e}")
                return chunk_trades
            time.sleep(2)
            continue

        if resp.status_code == 429:
            # Rate limit - bekle
            with progress_lock:
                print(f"  [Parca {chunk_id}/{total_chunks}] Rate limit (429)! 10 saniye bekleniyor...")
            time.sleep(10)
            continue

        if resp.status_code != 200:
            with progress_lock:
                print(f"  [Parca {chunk_id}/{total_chunks}] HTTP {resp.status_code}: {resp.text[:200]}")
            return chunk_trades

        data = resp.json()

        if isinstance(data, dict) and "code" in data:
            with progress_lock:
                print(f"  [Parca {chunk_id}/{total_chunks}] API Hatasi: {data.get('msg', data)}")
            return chunk_trades

        if not data:
            break

        chunk_trades.extend(data)
        
        with progress_lock:
            total_requests += 1
            fetched_trades_count += len(data)
            
            # Ilerleme raporu (her 20 istekte bir)
            if total_requests % 20 == 0:
                trade_time = datetime.fromtimestamp(data[-1]["T"] / 1000, tz=timezone.utc)
                print(f"  Istek #{total_requests:>4} | Toplam Islem: {fetched_trades_count:>8,} | "
                      f"Son Zaman: {trade_time.strftime('%H:%M:%S')} UTC")

        last_time = data[-1]["T"]
        if len(data) < 1000:
            current_start = last_time + 1
        else:
            current_start = last_time

        time.sleep(0.05)
        retry_count = 0
        
    return chunk_trades


def fetch_aggtrades(symbol, start_ms, end_ms):
    """Binance Spot aggTrades endpoint'inden parcali/paralel sekilde tum islemleri ceker."""
    global total_requests, fetched_trades_count
    total_requests = 0
    fetched_trades_count = 0
    
    cpu_count = os.cpu_count() or 2
    cpu_half = max(1, cpu_count // 2)
    max_workers = min(cpu_half, 8)
        
    chunk_size_ms = 30 * 60 * 1000 # 30 mins
    
    print(f"\n  Zaman araligi: {START_DT} -> {END_DT}")
    print(f"  Toplam sure: {(end_ms - start_ms) / 1000} saniye")
    print(f"  Thread Sayisi: {max_workers} (CPU: {cpu_count})")
    print(f"  Strateji: {chunk_size_ms/60000:.0f} dakikalik parcalarla paralel cekim\n")

    chunks = []
    curr = start_ms
    while curr < end_ms:
        nxt = min(curr + chunk_size_ms, end_ms)
        chunks.append((curr, nxt))
        curr = nxt
        
    all_trades = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for i, (c_start, c_end) in enumerate(chunks):
            futures.append(executor.submit(fetch_chunk, symbol, c_start, c_end, i+1, len(chunks)))
            
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res:
                all_trades.extend(res)
                
    # Sort by time
    if all_trades:
        all_trades.sort(key=lambda x: x["T"])
                
    return all_trades


def trades_to_1s_ohlcv(trades_list, start_ms, end_ms):
    """Ham islem verisini 1 sanyelik OHLCV mumlara donusturur."""

    if not trades_list:
        return None

    # DataFrame olustur
    df = pd.DataFrame(trades_list)
    # Mükerrer verileri (ayni islem ID'ye sahip olanlari) sil
    initial_len = len(df)
    df.drop_duplicates(subset=["a"], inplace=True)
    if len(df) < initial_len:
        print(f"  Bilgi: {initial_len - len(df)} adet mükerrer islem silindi.")

    df["T"] = pd.to_datetime(df["T"], unit="ms", utc=True)
    df["p"] = df["p"].astype(float)
    df["q"] = df["q"].astype(float)
    df["m"] = df["m"].astype(bool)  # True = buyer is maker (market sell)

    # Ham veriyi kaydet
    print(f"\n  Ham islem verisi kaydediliyor ({len(df)} islem)...")
    raw_save = df.copy()
    raw_save["T_str"] = raw_save["T"].astype(str)
    raw_save[["a", "p", "q", "f", "l", "T_str", "m"]].to_csv(RAW_TRADES_CSV, index=False)
    print(f"  Kaydedildi: {RAW_TRADES_CSV}")

    # 1s OHLCV olustur
    print("\n  1 sanyelik OHLCV mumlara donusturuluyor...")
    df.set_index("T", inplace=True)

    ohlcv = pd.DataFrame()
    ohlcv["open"] = df["p"].resample("1s").first()
    ohlcv["high"] = df["p"].resample("1s").max()
    ohlcv["low"] = df["p"].resample("1s").min()
    ohlcv["close"] = df["p"].resample("1s").last()
    ohlcv["volume"] = df["q"].resample("1s").sum()

    # Alis/Satis ayirimi
    # m=True -> buyer is maker -> market SELL (satis baskisi)
    # m=False -> seller is maker -> market BUY (alis baskisi)
    sell_vol = df[df["m"] == True]["q"].resample("1s").sum()
    buy_vol = df[df["m"] == False]["q"].resample("1s").sum()
    ohlcv["sell_volume"] = sell_vol
    ohlcv["buy_volume"] = buy_vol
    ohlcv["trades_count"] = df["p"].resample("1s").count()

    # NaN satirlari (islem olmayan saniyeler) onceki kapanis ile doldur
    ohlcv["open"] = ohlcv["open"].ffill()
    ohlcv["high"] = ohlcv["high"].ffill()
    ohlcv["low"] = ohlcv["low"].ffill()
    ohlcv["close"] = ohlcv["close"].ffill()
    ohlcv[["volume", "sell_volume", "buy_volume", "trades_count"]] = \
        ohlcv[["volume", "sell_volume", "buy_volume", "trades_count"]].fillna(0)

    # Zaman araligini filtrele
    start_dt = pd.Timestamp(start_ms, unit="ms", tz="UTC")
    end_dt = pd.Timestamp(end_ms, unit="ms", tz="UTC")
    ohlcv = ohlcv[(ohlcv.index >= start_dt) & (ohlcv.index <= end_dt)]

    ohlcv = ohlcv.dropna(subset=["open"])
    ohlcv = ohlcv.reset_index()
    ohlcv.rename(columns={"T": "open_time"}, inplace=True)

    return ohlcv


def main():
    print("=" * 65)
    print(f"  {SYMBOL} Spot AggTrades -> 1s OHLCV")
    print(f"  Tarih : 10 Ekim 2025")
    print(f"  Saat  : 20:00:00 - 23:59:59 UTC")
    print("=" * 65)

    # 1) AggTrades cek
    print("\n[1/3] Spot aggTrades cekiliyor...")
    trades = fetch_aggtrades(SYMBOL, START_MS, END_MS)

    if not trades:
        print("\nHATA: Islem verisi cekilemedi!")
        return

    print(f"\n  Toplam {len(trades):,} islem cekildi.")

    # 2) 1s OHLCV'ye donustur
    print("\n[2/3] 1s OHLCV donusumu...")
    ohlcv = trades_to_1s_ohlcv(trades, START_MS, END_MS)

    if ohlcv is None or len(ohlcv) == 0:
        print("\nHATA: OHLCV donusumu basarisiz!")
        return

    # CSV kaydet
    ohlcv.to_csv(SPOT_1S_CSV, index=False)
    print(f"\n  Spot 1s OHLCV kaydedildi: {SPOT_1S_CSV}")

    # 3) Ozet
    print("\n[3/3] OZET")
    print("=" * 65)
    print(f"  Toplam islem sayisi  : {len(trades):,}")
    print(f"  Toplam 1s mum sayisi : {len(ohlcv):,}")
    print(f"  Ilk zaman            : {ohlcv['open_time'].iloc[0]}")
    print(f"  Son zaman            : {ohlcv['open_time'].iloc[-1]}")
    print(f"  Acilis fiyati        : {ohlcv['close'].iloc[0]:.6f} USDT")
    print(f"  Kapanis fiyati       : {ohlcv['close'].iloc[-1]:.6f} USDT")
    print(f"  En dusuk             : {ohlcv['low'].min():.6f} USDT")
    print(f"  En yuksek            : {ohlcv['high'].max():.6f} USDT")

    change = ((ohlcv['close'].iloc[-1] / ohlcv['close'].iloc[0]) - 1) * 100
    print(f"  Degisim              : {change:+.3f}%")

    # Alim/Satim istatistikleri
    total_buy = ohlcv['buy_volume'].sum()
    total_sell = ohlcv['sell_volume'].sum()
    total_vol = total_buy + total_sell
    print(f"\n  Toplam hacim         : {total_vol:,.0f} {SYMBOL[:-4] if SYMBOL.endswith('USDT') else SYMBOL}")
    print(f"  Alis hacmi           : {total_buy:,.0f} ({total_buy/total_vol*100:.1f}%)")
    print(f"  Satis hacmi          : {total_sell:,.0f} ({total_sell/total_vol*100:.1f}%)")
    print("=" * 65)


if __name__ == "__main__":
    main()
