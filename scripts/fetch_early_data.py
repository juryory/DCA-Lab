"""
抓取历史日线数据，支持以下品种:
  - AU9999  (上海黄金交易所 Au99.99)
  - 沪深300 (000300.SH)
  - 标普500 (SPX, 通过 tushare 全球指数接口)

使用 tushare pro 接口，需要设置环境变量 TUSHARE_TOKEN

用法:
  pip install tushare pandas
  export TUSHARE_TOKEN=你的token

  # 拉取全部品种
  python scripts/fetch_early_data.py

  # 只拉取指定品种
  python scripts/fetch_early_data.py au9999
  python scripts/fetch_early_data.py csi300
  python scripts/fetch_early_data.py sp500

  # 自定义时间范围
  python scripts/fetch_early_data.py au9999 20080101 20171218
"""

import os
import sys
import time
import tushare as ts
import pandas as pd

TOKEN = os.environ.get("TUSHARE_TOKEN", "")
if not TOKEN:
    TOKEN = input("请输入 Tushare Token: ").strip()
if not TOKEN:
    print("Token 不能为空")
    sys.exit(1)

ts.set_token(TOKEN)
pro = ts.pro_api()

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)


def merge_and_save(new_df, outpath):
    """增量合并：读取已有文件，合并新数据，去重后保存"""
    if os.path.exists(outpath):
        try:
            existing = pd.read_csv(outpath, dtype=str)
            combined = pd.concat([existing, new_df.astype(str)], ignore_index=True)
            combined = combined.drop_duplicates(subset=["trade_date"], keep="last")
        except Exception:
            combined = new_df
    else:
        combined = new_df
    combined = combined.sort_values("trade_date").reset_index(drop=True)
    combined.to_csv(outpath, index=False, encoding="utf-8")
    print(f"saved {len(combined)} rows (merged) -> {outpath}")
    return combined


def fetch_au9999(start="20080101", end="20171218"):
    """抓取 AU99.99 期货日线 (上海黄金交易所)"""
    print(f"\n=== AU9999 ({start} ~ {end}) ===")
    frames = []
    sy, ey = int(start[:4]), int(end[:4])
    for year in range(sy, ey + 1):
        s = f"{year}0101" if year > sy else start
        e = f"{year}1231" if year < ey else end
        print(f"  {s} ~ {e} ...", end=" ", flush=True)
        try:
            df = pro.fut_daily(ts_code="AU99.99.SGE", start_date=s, end_date=e)
            if df is not None and not df.empty:
                frames.append(df)
                print(f"{len(df)} rows")
            else:
                print("no data")
        except Exception as ex:
            print(f"failed: {ex}")
        time.sleep(0.5)

    if not frames:
        print("AU9999: no data fetched")
        return None

    result = pd.concat(frames, ignore_index=True)
    result = result.sort_values("trade_date").drop_duplicates(subset=["trade_date"], keep="first")

    # 统一输出格式: trade_date,open,high,low,close
    out = pd.DataFrame({
        "trade_date": result["trade_date"],
        "open": result["open"],
        "high": result["high"],
        "low": result["low"],
        "close": result["close"],
    })
    outpath = os.path.join(DATA_DIR, "au9999_history.csv")
    merge_and_save(out, outpath)
    return out


def fetch_csi300(start="20050101", end=None):
    """抓取沪深300指数日线 (000300.SH)"""
    if end is None:
        end = time.strftime("%Y%m%d")
    print(f"\n=== CSI300 / 沪深300 ({start} ~ {end}) ===")
    frames = []
    sy, ey = int(start[:4]), int(end[:4])
    for year in range(sy, ey + 1):
        s = f"{year}0101" if year > sy else start
        e = f"{year}1231" if year < ey else end
        print(f"  {s} ~ {e} ...", end=" ", flush=True)
        try:
            df = pro.index_daily(ts_code="000300.SH", start_date=s, end_date=e)
            if df is not None and not df.empty:
                frames.append(df)
                print(f"{len(df)} rows")
            else:
                print("no data")
        except Exception as ex:
            print(f"failed: {ex}")
        time.sleep(0.5)

    if not frames:
        print("CSI300: no data fetched")
        return None

    result = pd.concat(frames, ignore_index=True)
    result = result.sort_values("trade_date").drop_duplicates(subset=["trade_date"], keep="first")

    out = pd.DataFrame({
        "trade_date": result["trade_date"],
        "open": result["open"],
        "high": result["high"],
        "low": result["low"],
        "close": result["close"],
    })
    outpath = os.path.join(DATA_DIR, "csi300_history.csv")
    merge_and_save(out, outpath)
    return out


def fetch_sp500(start="20000101", end=None):
    """抓取标普500指数日线 (通过 tushare 全球指数接口)"""
    if end is None:
        end = time.strftime("%Y%m%d")
    print(f"\n=== S&P 500 ({start} ~ {end}) ===")

    # tushare 全球指数接口: index_global
    # 标普500 的 ts_code 为 SPX (或 .INX)
    frames = []
    sy, ey = int(start[:4]), int(end[:4])
    for year in range(sy, ey + 1):
        s = f"{year}0101" if year > sy else start
        e = f"{year}1231" if year < ey else end
        print(f"  {s} ~ {e} ...", end=" ", flush=True)
        try:
            df = pro.index_global(ts_code="SPX", start_date=s, end_date=e)
            if df is not None and not df.empty:
                frames.append(df)
                print(f"{len(df)} rows")
            else:
                print("no data")
        except Exception as ex:
            print(f"failed: {ex}")
        time.sleep(0.5)

    if not frames:
        print("SP500: no data fetched")
        print("tip: tushare index_global may require higher permission level")
        print("     alternative ts_codes to try: SPX, .INX, SP500")
        return None

    result = pd.concat(frames, ignore_index=True)
    result = result.sort_values("trade_date").drop_duplicates(subset=["trade_date"], keep="first")

    out = pd.DataFrame({
        "trade_date": result["trade_date"],
        "open": result["open"],
        "high": result["high"],
        "low": result["low"],
        "close": result["close"],
    })
    outpath = os.path.join(DATA_DIR, "sp500_history.csv")
    merge_and_save(out, outpath)
    return out


# ---- CLI entry ----
if __name__ == "__main__":
    args = sys.argv[1:]
    target = args[0].lower() if args else "all"
    custom_start = args[1] if len(args) > 1 else None
    custom_end = args[2] if len(args) > 2 else None

    if target in ("all", "au9999"):
        fetch_au9999(
            start=custom_start or "20080101",
            end=custom_end or "20171218",
        )

    if target in ("all", "csi300"):
        fetch_csi300(
            start=custom_start or "20050101",
            end=custom_end,
        )

    if target in ("all", "sp500"):
        fetch_sp500(
            start=custom_start or "20000101",
            end=custom_end,
        )

    if target not in ("all", "au9999", "csi300", "sp500"):
        print(f"unknown target: {target}")
        print("usage: python fetch_early_data.py [all|au9999|csi300|sp500] [start] [end]")
        sys.exit(1)

    print("\ndone.")
