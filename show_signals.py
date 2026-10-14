"""
show_signals.py — CLI to inspect bot_data.sqlite3: recent scans (every symbol,
every cycle, with the reason it did or didn't fire), fired signals (with
entry/SL/TP preview), and trades.

Run in the same folder as bot.py: python show_signals.py [N]
"""
import sqlite3
import sys

DB_PATH = "bot_data.sqlite3"


def main():
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    print(f"\n--- Last {limit} scans (every symbol checked, every cycle) ---")
    rows = conn.execute(
        "SELECT ts, symbol, side, reason, adx, atr FROM scans ORDER BY id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    if not rows:
        print("(none logged yet — bot hasn't completed a cycle, or log_all_scans is off)")
    for r in rows:
        side = r["side"] or "----"
        print(f"{r['ts']}  {side:<4} {r['symbol']:<8} ADX={r['adx']:.1f}  ATR={r['atr']:.5f}  {r['reason']}")

    print(f"\n--- Last {limit} fired signals (entry/SL/TP preview) ---")
    rows = conn.execute(
        "SELECT ts, symbol, side, adx, entry_price, sl, tp FROM signals ORDER BY id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    if not rows:
        print("(none logged yet)")
    for r in rows:
        if r["entry_price"] is not None:
            print(f"{r['ts']}  {r['side']:<4} {r['symbol']:<8} ADX={r['adx']:.1f}  "
                  f"Entry={r['entry_price']:.5f}  SL={r['sl']:.5f}  TP={r['tp']:.5f}")
        else:
            print(f"{r['ts']}  {r['side']:<4} {r['symbol']:<8} ADX={r['adx']:.1f}  (no price preview logged)")

    print(f"\n--- Last {limit} trade attempts ---")
    rows = conn.execute(
        "SELECT ts, symbol, side, volume, price, sl, tp, retcode, comment FROM trades ORDER BY id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    if not rows:
        print("(none logged yet)")
    for r in rows:
        status = "FILLED" if r["retcode"] == 10009 else f"retcode={r['retcode']} ({r['comment']})"
        print(f"{r['ts']}  {r['side']:<4} {r['symbol']:<8} {r['volume']} lots @ {r['price']:.5f}  "
              f"SL={r['sl']:.5f}  TP={r['tp']:.5f}  {status}")

    conn.close()


if __name__ == "__main__":
    main()