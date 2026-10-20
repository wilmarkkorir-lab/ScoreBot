"""
check_mt5.py — Live MT5 connection + trade capability test
Uses same init logic as bot.py
Run: python check_mt5.py
"""
import os, sys
from dotenv import load_dotenv
load_dotenv()

try:
    import MetaTrader5 as mt5
except ImportError:
    sys.exit("ERROR: MetaTrader5 not installed. Run: pip install MetaTrader5")

MT5_PATH     = os.getenv("MT5_PATH", "")
MT5_LOGIN    = int(os.getenv("MT5_LOGIN", "0"))
MT5_PASSWORD = os.getenv("MT5_PASSWORD", "")
MT5_SERVER   = os.getenv("MT5_SERVER", "")

print("=" * 55)
print("  KORIRBOT — MT5 Connection & Trade Capability Check")
print("=" * 55)

# Step 1: initialize — try .env path first, then let MT5 find running terminal
kwargs = {"path": MT5_PATH} if MT5_PATH else {}
if not mt5.initialize(**kwargs):
    # fallback: try without path (connects to any already-running terminal)
    if not mt5.initialize():
        err = mt5.last_error()
        print(f"[FAIL] mt5.initialize() — {err}")
        print("\n  Fix: Open MetaTrader 5 and log in first, then re-run.")
        sys.exit(1)

term = mt5.terminal_info()
print(f"[OK]   Terminal: {term.company} | {term.name}")
print(f"       Path    : {term.path}")
print(f"       Connected: {term.connected} | Trade allowed: {term.trade_allowed}")

# Step 2: login with .env credentials if provided
if MT5_LOGIN and MT5_PASSWORD and MT5_SERVER:
    if not mt5.login(MT5_LOGIN, password=MT5_PASSWORD, server=MT5_SERVER):
        err = mt5.last_error()
        print(f"[WARN] .env login failed ({err}) — using terminal's active session")

# Step 3: account info
info = mt5.account_info()
if info is None:
    print("[FAIL] No account info — not logged in")
    mt5.shutdown()
    sys.exit(1)

print(f"[OK]   Account #{info.login} | {info.server}")
print(f"       Balance : {info.balance:.2f} {info.currency}")
print(f"       Equity  : {info.equity:.2f} {info.currency}")
print(f"       Leverage: 1:{info.leverage}")
print(f"       trade_allowed: {info.trade_allowed} | trade_expert: {info.trade_expert}")

issues = []
if not info.trade_allowed:
    issues.append("trade_allowed=False — enable AutoTrading button in MT5 toolbar (Ctrl+E)")
if not info.trade_expert:
    issues.append("trade_expert=False — go to Tools > Options > Expert Advisors > Allow automated trading")
if not term.connected:
    issues.append("Terminal not connected to broker — check internet connection")
if not term.trade_allowed:
    issues.append("Terminal trade_allowed=False — click the AutoTrading button in MT5")

# Step 4: symbol tick check
tick = mt5.symbol_info_tick("EURUSD")
if tick:
    print(f"[OK]   EURUSD tick — bid={tick.bid} ask={tick.ask}")
else:
    issues.append("EURUSD tick unavailable — right-click Market Watch > Show All")

print("-" * 55)
if issues:
    print("[ISSUES FOUND]")
    for i, issue in enumerate(issues, 1):
        print(f"  {i}. {issue}")
    print("\nBot will NOT trade until these are resolved.")
else:
    print("[ALL CHECKS PASSED] — Bot is ready to trade automatically.")

mt5.shutdown()
