"""
bot.py — KORIRBOT: Supply & Demand Confluence Engine
=====================================================
STRATEGY: Institutional Supply & Demand (S&D)
  Step 1 — Zone Detection  : 4H/1H DBR/RBD/RBR/DBD patterns
                              (Displacement >= 2x ATR + FVG + BOS)
  Step 2 — Liquidity Sweep : 15M Asian Range high/low sweep verification
  Step 3 — CHoCH Entry     : 5M Change of Character confirmation
  Execution                : Limit order at zone base, SL below/above zone wick

RISK:
  - 1% equity risk per trade (dynamic lot sizing)
  - TP1 at 1:2 RR — close 50% + move SL to breakeven
  - TP2 at 1:5 RR or opposing HTF S&D zone
  - 3% daily circuit breaker
  - Spread cap: 1.5 pips majors / 2.5 pips minors
  - 30/30 news blackout rule

SETUP:
  pip install MetaTrader5 pandas numpy requests python-dotenv defusedxml
  .env: MT5_LOGIN, MT5_PASSWORD, MT5_SERVER, MT5_PATH,
        TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
  Run: python bot.py
"""

import argparse
import logging
import os
import re
import sqlite3
import sys
import time
import traceback
try:
    import defusedxml.ElementTree as ET
except ImportError:
    sys.exit("[ERROR] defusedxml is required: pip install defusedxml")
from datetime import datetime, timedelta, timezone

import MetaTrader5 as mt5
import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# CONFIG
# =============================================================================
CONFIG = {
    "symbols": [
        "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD",
        "EURGBP", "EURJPY", "EURCHF", "EURAUD", "EURCAD", "EURNZD",
        "GBPJPY", "GBPCHF", "GBPAUD", "GBPCAD", "GBPNZD",
        "AUDJPY", "AUDCHF", "AUDCAD", "AUDNZD",
        "CADJPY", "CADCHF", "NZDJPY", "NZDCHF", "CHFJPY",
        "XAUUSD", "XAGUSD",
    ],
    # Timeframes
    "htf":     "H4",   # S&D zone detection
    "itf":     "H1",   # Intermediate confirmation
    "mtf":     "M15",  # Liquidity sweep
    "etf":     "M5",   # CHoCH entry confirmation
    "exec_tf": "H1",   # Execution / indicators
    # Indicators
    "ema_fast": 50, "ema_slow": 200,
    "atr_period": 14,
    "adx_period": 14, "adx_threshold": 20,
    "rsi_period": 14,
    "macd_fast": 12, "macd_slow": 26, "macd_signal": 9,
    "bb_period": 20, "bb_std": 2.0,
    "volume_lookback": 20,
    # S&D Zone detection
    "sd_displacement_atr_mult": 2.0,  # displacement body >= 2x ATR
    "sd_base_candles_max": 3,         # max base candles
    "sd_base_body_atr_pct": 0.5,      # base body <= 50% ATR (small candles)
    "sd_zone_expiry_bars": 50,        # H4 bars before zone expires unused
    "sd_bos_lookback": 20,            # bars to find swing high/low for BOS
    "sd_choch_lookback": 10,          # bars for CHoCH on M5
    # Risk — S&D Blueprint
    "risk_per_trade_pct": 1.0,
    "atr_sl_multiplier": 1.5,
    "news_atr_sl_multiplier": 2.5,
    "risk_reward_be": 1.0,            # move SL to BE at 1:1
    "risk_reward_tp1": 2.0,           # TP1 at 1:2 — close 50%
    "risk_reward_tp2": 5.0,           # TP2 at 1:5 or opposing zone
    "risk_reward_ratio": 5.0,
    "risk_reward_trail": 2.0,
    "tp1_close_pct": 0.5,             # close 50% at TP1
    "max_daily_loss_pct": 3.0,
    "drawdown_warning_pct": 2.0,
    "max_open_trades": 6,
    "max_open_per_currency": 2,
    "max_positions_per_symbol": 1,
    "magic_number": 123456,
    "poll_seconds": 30,
    # Spread caps — S&D blueprint
    "max_spread_pips": 1.5,
    "symbol_spread_pips": {
        "XAUUSD": 5.0, "XAGUSD": 8.0,
        "GBPJPY": 2.5, "EURJPY": 2.5, "USDJPY": 2.5,
        "AUDJPY": 2.5, "CADJPY": 2.5, "NZDJPY": 2.5, "CHFJPY": 2.5,
        "GBPNZD": 2.5, "EURNZD": 2.5, "AUDNZD": 2.5,
        "GBPAUD": 2.5, "GBPCAD": 2.5, "EURAUD": 2.5, "EURCAD": 2.5,
    },
    "cooldown_minutes_after_loss": 15,
    "enable_trailing_stop": True,
    "trailing_step_atr": 0.5,
    "breakeven_buffer_pips": 2,
    "max_order_retries": 3,
    "max_slippage_points": 20,
    "enable_pyramiding": False,
    "pyramid_levels": [
        {"atr_multiplier": 1.0, "lot_multiplier": 0.5},
        {"atr_multiplier": 2.0, "lot_multiplier": 0.3},
    ],
    "log_all_scans": True,
    "telegram_notify_on_signal": True,
    "telegram_heartbeat_hours": 1,
    "daily_summary_hour_utc": 21,
    "weekly_report_day": 4,
    "weekly_report_hour_utc": 20,
    # News / calendar
    "news_blackout_minutes": 30,
    "news_resume_minutes": 30,
    "news_breakout_lookback_bars": 3,
    "news_breakout_atr_multiplier": 0.5,
    "news_lot_multiplier": 0.5,
    "low_confidence_threshold": 65,
    "news_symbols": ["XAUUSD","EURUSD","GBPUSD","USDJPY","USDCAD","AUDUSD","NZDUSD","USDCHF"],
    "calendar_fetch_interval_minutes": 60,
    "high_impact_keywords": [
        "CPI","NFP","Non-Farm","FOMC","Fed","Interest Rate","GDP",
        "PMI","Retail Sales","Unemployment","Inflation","PPI",
        "Trade Balance","Current Account","Housing","Durable Goods",
        "Consumer Confidence","ISM","ADP","Jobless Claims",
    ],
    "fvg_min_size_atr_pct": 0.3,
    "fvg_expiry_bars": 10,
    "fvg_reversal_bars": 3,
    "calendar_fallback_events": [],
    "startup_events_lookahead_minutes": 120,
    # Asian range
    "asian_range_start_hour": 22,
    "asian_range_end_hour": 6,
    "liquidity_sweep_atr_pct": 0.5,
    "symbol_configs": {
        "XAUUSD": {"min_atr_value": 1.0},
        "XAGUSD": {"min_atr_value": 0.05},
        "USDJPY": {"min_atr_value": 0.01}, "EURJPY": {"min_atr_value": 0.01},
        "GBPJPY": {"min_atr_value": 0.01}, "AUDJPY": {"min_atr_value": 0.01},
        "CADJPY": {"min_atr_value": 0.01}, "NZDJPY": {"min_atr_value": 0.01},
        "CHFJPY": {"min_atr_value": 0.01},
    },
    "min_atr_value": 0.0001,
    "telegram": {
        "enabled": True,
        "bot_token": os.getenv("TELEGRAM_BOT_TOKEN", ""),
        "chat_id": os.getenv("TELEGRAM_CHAT_ID", ""),
    },
    "mt5": {
        "login": int(os.getenv("MT5_LOGIN", "0")),
        "password": os.getenv("MT5_PASSWORD", ""),
        "server": os.getenv("MT5_SERVER", ""),
        "path": os.getenv("MT5_PATH", ""),
    },
    "min_lot_size": 0.01,
    "max_lot_size": 5.0,
    "db_path": "bot_data.sqlite3",
    "log_path": "bot.log",
}

# =============================================================================
# LOGGING
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(CONFIG["log_path"], encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("bot")

TIMEFRAME_MAP = {
    "M1": mt5.TIMEFRAME_M1, "M5": mt5.TIMEFRAME_M5, "M15": mt5.TIMEFRAME_M15,
    "M30": mt5.TIMEFRAME_M30, "H1": mt5.TIMEFRAME_H1, "H4": mt5.TIMEFRAME_H4,
    "D1": mt5.TIMEFRAME_D1,
}
NON_RETRYABLE = {10018, 10019, 10027, 10031, 10004}
USD_POSITIVE  = {"USDJPY", "USDCAD", "USDCHF"}

# =============================================================================
# KILLZONES & SESSIONS
# =============================================================================
KILLZONES = {
    "Asian Range":  {"start": 22, "end": 6,  "trade": False, "emoji": "🌏"},
    "London":       {"start": 7,  "end": 10, "trade": True,  "emoji": "🏦"},
    "New York":     {"start": 12, "end": 15, "trade": True,  "emoji": "🗽"},
    "London Close": {"start": 15, "end": 16, "trade": False, "emoji": "🔔"},
}
SESSIONS = {
    "Sydney":   {"open": 21, "close": 6,  "emoji": "🦘",
                 "pairs": ["AUDUSD","NZDUSD","AUDJPY","AUDNZD"]},
    "Tokyo":    {"open": 0,  "close": 9,  "emoji": "🗼",
                 "pairs": ["USDJPY","EURJPY","GBPJPY","AUDJPY","CADJPY"]},
    "London":   {"open": 7,  "close": 16, "emoji": "🏦",
                 "pairs": ["EURUSD","GBPUSD","EURGBP","EURCHF","GBPCHF"]},
    "New York": {"open": 12, "close": 21, "emoji": "🗽",
                 "pairs": ["EURUSD","GBPUSD","USDJPY","USDCAD","XAUUSD"]},
}

# =============================================================================
# STATE
# =============================================================================
class _State:
    def __init__(self):
        self.cooldown_until         = {}
        self.pyramid_applied        = {}
        self.last_session           = None
        self.last_killzone          = None
        self.last_heartbeat         = None
        self.last_daily_summary     = None
        self.last_weekly_report     = None
        self.drawdown_warned        = False
        self.calendar_cache         = []
        self.calendar_fetched_at    = None
        self.predicted_events       = set()
        self.asian_ranges           = {}
        self.sd_zones               = {}   # symbol -> list of zone dicts
        self.mitigated_zones        = {}   # symbol -> set of zone ids
        self.last_events_alert_keys = set()

S = _State()


# =============================================================================
# TELEGRAM
# =============================================================================
def telegram_send(text: str) -> None:
    tg = CONFIG["telegram"]
    if not tg["enabled"] or not tg["bot_token"] or not tg["chat_id"]:
        return
    for attempt in range(3):
        try:
            url = f"https://api.telegram.org/bot{tg['bot_token']}/sendMessage"
            requests.post(url, data={"chat_id": tg["chat_id"], "text": text}, timeout=10)
            return
        except Exception as e:
            if attempt == 2:
                log.warning(f"[telegram] {e}")
            else:
                time.sleep(2)


# =============================================================================
# SQLITE
# =============================================================================
def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(CONFIG["db_path"])
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, symbol TEXT, side TEXT,
            strategy TEXT, entry_price REAL, sl REAL, tp1 REAL, tp2 REAL,
            adx REAL, rsi REAL, zone_top REAL, zone_bottom REAL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS scans (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, symbol TEXT,
            side TEXT, reason TEXT, adx REAL, atr REAL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, symbol TEXT, side TEXT,
            volume REAL, price REAL, sl REAL, tp REAL, retcode INTEGER,
            comment TEXT, order_id INTEGER)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS daily_pnl (
            date TEXT PRIMARY KEY, start_balance REAL, realized_pnl REAL,
            halted INTEGER DEFAULT 0)""")
        # Migrations
        existing = {row[1] for row in conn.execute("PRAGMA table_info(signals)")}
        for col, typedef in [
            ("strategy",    "TEXT"), ("tp1",        "REAL"),
            ("tp2",         "REAL"), ("adx",        "REAL"),
            ("rsi",         "REAL"), ("zone_top",   "REAL"),
            ("zone_bottom", "REAL"),
        ]:
            if col not in existing:
                conn.execute(f"ALTER TABLE signals ADD COLUMN {col} {typedef}")
        conn.commit()
    except Exception:
        conn.close()
        raise
    return conn


def log_signal(conn, symbol, side, strategy, entry, sl, tp1, tp2, adx, rsi,
               zone_top=None, zone_bottom=None):
    conn.execute(
        """INSERT INTO signals
           (ts,symbol,side,strategy,entry_price,sl,tp1,tp2,adx,rsi,zone_top,zone_bottom)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (datetime.now(timezone.utc).isoformat(), symbol, side, strategy,
         entry, sl, tp1, tp2, adx, rsi, zone_top, zone_bottom))
    conn.commit()


def log_scan(conn, symbol, side, reason, adx, atr):
    if not CONFIG["log_all_scans"]:
        return
    conn.execute(
        "INSERT INTO scans (ts,symbol,side,reason,adx,atr) VALUES (?,?,?,?,?,?)",
        (datetime.now(timezone.utc).isoformat(), symbol, side, reason, adx, atr))
    conn.commit()


def log_trade(conn, symbol, side, volume, price, sl, tp, retcode, comment, order_id):
    conn.execute(
        """INSERT INTO trades
           (ts,symbol,side,volume,price,sl,tp,retcode,comment,order_id)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (datetime.now(timezone.utc).isoformat(), symbol, side, volume,
         price, sl, tp, retcode, comment, order_id))
    conn.commit()


def update_realized_pnl(conn):
    today_start = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0)
    deals = mt5.history_deals_get(today_start, datetime.now(timezone.utc))
    if not deals:
        return
    realized = sum(
        d.profit for d in deals
        if d.magic == CONFIG["magic_number"] and d.entry == mt5.DEAL_ENTRY_OUT
    )
    today = today_start.date().isoformat()
    conn.execute("UPDATE daily_pnl SET realized_pnl=? WHERE date=?", (realized, today))
    conn.commit()


# =============================================================================
# DAILY LOSS / DRAWDOWN
# =============================================================================
def get_or_init_daily_row(conn) -> sqlite3.Row:
    today = datetime.now(timezone.utc).date().isoformat()
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM daily_pnl WHERE date=?", (today,)).fetchone()
    if row is None:
        info = mt5.account_info()
        bal  = info.balance if info else 0.0
        conn.execute(
            "INSERT INTO daily_pnl (date,start_balance,realized_pnl,halted) VALUES (?,?,?,0)",
            (today, bal, 0.0))
        conn.commit()
        row = conn.execute("SELECT * FROM daily_pnl WHERE date=?", (today,)).fetchone()
    return row


def trading_halted_for_today(conn) -> bool:
    row  = get_or_init_daily_row(conn)
    info = mt5.account_info()
    if info is None or row["start_balance"] == 0:
        return False
    pct = (info.equity - row["start_balance"]) / row["start_balance"] * 100.0
    if not S.drawdown_warned and pct <= -CONFIG["drawdown_warning_pct"]:
        telegram_send(
            f"⚠️ DRAWDOWN WARNING: {pct:.2f}% today\n"
            f"Circuit breaker triggers at -{CONFIG['max_daily_loss_pct']}%"
        )
        S.drawdown_warned = True
    if pct <= -CONFIG["max_daily_loss_pct"]:
        if not row["halted"]:
            msg = (f"🛑 CIRCUIT BREAKER FIRED: {pct:.2f}% daily loss\n"
                   f"All entries locked until 00:00 UTC.")
            log.warning(msg)
            telegram_send(msg)
            _close_all_positions_to_breakeven(conn)
            conn.execute("UPDATE daily_pnl SET halted=1 WHERE date=?",
                         (datetime.now(timezone.utc).date().isoformat(),))
            conn.commit()
        return True
    return False


def _close_all_positions_to_breakeven(conn):
    positions = mt5.positions_get()
    if not positions:
        return
    for p in positions:
        if p.magic != CONFIG["magic_number"]:
            continue
        modify_sltp(p, p.price_open)
        log.info(f"[circuit] {p.symbol} moved to breakeven")


# =============================================================================
# FOREX CALENDAR — 30/30 rule
# =============================================================================
def fetch_forex_calendar() -> list:
    now = datetime.now(timezone.utc)
    if (S.calendar_fetched_at is not None and
            (now - S.calendar_fetched_at).total_seconds() <
            CONFIG["calendar_fetch_interval_minutes"] * 60):
        return S.calendar_cache
    if (S.calendar_fetched_at is not None and
            (now - S.calendar_fetched_at).total_seconds() < 60):
        return S.calendar_cache
    try:
        resp = requests.get(
            "https://nfs.faireconomy.media/ff_calendar_thisweek.xml", timeout=15)
        resp.raise_for_status()
        root   = ET.fromstring(resp.content)
        events = []
        for item in root.findall(".//event"):
            try:
                title   = (item.findtext("title")   or "").strip()
                country = (item.findtext("country") or "").strip()
                impact  = (item.findtext("impact")  or "").strip()
                date_s  = (item.findtext("date")    or "").strip()
                time_s  = (item.findtext("time")    or "").strip()
                if impact.lower() != "high":
                    continue
                if not any(k.lower() in title.lower()
                           for k in CONFIG["high_impact_keywords"]):
                    continue
                dt_s = f"{date_s} {time_s}" if time_s else date_s
                try:
                    dt = datetime.strptime(dt_s, "%m-%d-%Y %I:%M%p").replace(
                        tzinfo=timezone.utc)
                except ValueError:
                    dt = datetime.strptime(dt_s, "%m-%d-%Y").replace(
                        tzinfo=timezone.utc)
                events.append((dt, title, country, impact))
            except Exception as parse_err:
                log.debug(f"[calendar] parse error: {parse_err}")
                continue
        S.calendar_cache      = events
        S.calendar_fetched_at = now
        log.info(f"[calendar] {len(events)} high-impact events this week")
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 429:
            log.warning("[calendar] rate-limited (429) — retry in 60s")
            S.calendar_fetched_at = now
        else:
            log.warning(f"[calendar] fetch failed: {e}")
        if not S.calendar_cache and CONFIG.get("calendar_fallback_events"):
            S.calendar_cache = CONFIG["calendar_fallback_events"]
    except Exception as e:
        log.warning(f"[calendar] fetch failed: {e}")
        if not S.calendar_cache and CONFIG.get("calendar_fallback_events"):
            S.calendar_cache = CONFIG["calendar_fallback_events"]
    return S.calendar_cache


def get_upcoming_events(lookahead_minutes: int = 60) -> list:
    now = datetime.now(timezone.utc)
    return [(dt, t, c) for dt, t, c, _ in fetch_forex_calendar()
            if timedelta(0) <= (dt - now) <= timedelta(minutes=lookahead_minutes)]


def news_blackout_active() -> bool:
    now     = datetime.now(timezone.utc)
    blackout = timedelta(minutes=CONFIG["news_blackout_minutes"])
    for dt, _, _, _ in fetch_forex_calendar():
        if -blackout <= (now - dt) < timedelta(0):
            return True
    return False


def news_resume_window_active():
    now    = datetime.now(timezone.utc)
    window = timedelta(minutes=CONFIG["news_resume_minutes"])
    for dt, title, country, _ in fetch_forex_calendar():
        if timedelta(0) <= (now - dt) < window:
            return dt, title, country
    return None


def send_upcoming_events_alert(lookahead: int = 35):
    events = get_upcoming_events(lookahead_minutes=lookahead)
    if not events:
        return
    new_keys = {f"{dt.isoformat()}_{title}" for dt, title, _ in events}
    if new_keys <= S.last_events_alert_keys:
        return
    S.last_events_alert_keys |= new_keys
    lines = [f"⚠️ HIGH-IMPACT NEWS IN {lookahead} MIN — BLACKOUT ACTIVE", ""]
    for dt, title, country in events:
        eat = (dt + timedelta(hours=3)).strftime("%H:%M EAT")
        lines.append(f"🔔 {title} ({country}) — {dt.strftime('%H:%M UTC')} / {eat}")
    lines += ["", "⏸ Entries paused 30 min before & 30 min after."]
    telegram_send("\n".join(lines))


# =============================================================================
# SESSIONS & KILLZONES
# =============================================================================
def get_current_sessions() -> list:
    hour   = datetime.now(timezone.utc).hour
    active = []
    for name, s in SESSIONS.items():
        o, c = s["open"], s["close"]
        if o < c:
            if o <= hour < c:
                active.append(name)
        else:
            if hour >= o or hour < c:
                active.append(name)
    return active


def get_active_killzone() -> str | None:
    hour = datetime.now(timezone.utc).hour
    for name, kz in KILLZONES.items():
        s, e = kz["start"], kz["end"]
        if s < e:
            if s <= hour < e:
                return name
        else:
            if hour >= s or hour < e:
                return name
    return None


def in_tradeable_killzone() -> bool:
    kz = get_active_killzone()
    return kz is not None and KILLZONES[kz]["trade"]


def check_session_change():
    now_str = datetime.now(timezone.utc).strftime("%H:%M UTC")
    eat_str = (datetime.now(timezone.utc) + timedelta(hours=3)).strftime("%H:%M EAT")
    current = set(get_current_sessions())
    if S.last_session is None:
        S.last_session = current
    else:
        for name in current - S.last_session:
            s = SESSIONS[name]
            telegram_send(
                f"{s['emoji']} {name.upper()} SESSION OPENED\n"
                f"🕐 {now_str} / {eat_str}\n"
                f"📈 Best pairs: {' | '.join(s['pairs'])}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"S&D zones mapped. Scanning for liquidity sweeps."
            )
            log.info(f"[session] {name} opened")
        for name in S.last_session - current:
            s = SESSIONS[name]
            telegram_send(
                f"{s['emoji']} {name.upper()} SESSION CLOSED — {now_str} / {eat_str}"
            )
        S.last_session = current
    kz = get_active_killzone()
    if kz != S.last_killzone:
        if kz is not None:
            info   = KILLZONES[kz]
            status = "ACTIVE — scanning S&D zones" if info["trade"] else "MONITORING only"
            telegram_send(
                f"{info['emoji']} {kz.upper()} KILLZONE\n"
                f"🕐 {now_str} / {eat_str}\n"
                f"⚡ Status: {status}\n"
                f"📊 Tracking: {len(CONFIG['symbols'])} pairs"
            )
        S.last_killzone = kz


# =============================================================================
# ASIAN RANGE
# =============================================================================
def update_asian_range(symbol: str):
    from datetime import time as dtime
    now   = datetime.now(timezone.utc)
    today = now.date()
    cached = S.asian_ranges.get(symbol)
    if cached and cached.get("date") == today:
        return cached
    tf    = TIMEFRAME_MAP["M15"]
    rates = mt5.copy_rates_from_pos(symbol, tf, 0, 200)
    if rates is None or len(rates) < 10:
        return None
    df = pd.DataFrame(rates)
    df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
    asian_start = datetime.combine(today, dtime(0, 0), tzinfo=timezone.utc) \
                  - timedelta(hours=(24 - CONFIG["asian_range_start_hour"]))
    asian_end   = datetime.combine(today, dtime(CONFIG["asian_range_end_hour"], 0),
                                   tzinfo=timezone.utc)
    mask    = (df["time"] >= asian_start) & (df["time"] < asian_end)
    session = df[mask]
    if len(session) < 4:
        return None
    result = {
        "high": float(session["high"].max()),
        "low":  float(session["low"].min()),
        "date": today,
    }
    S.asian_ranges[symbol] = result
    return result


def liquidity_swept(symbol: str, side: str) -> bool:
    ar = update_asian_range(symbol)
    if ar is None:
        return False
    rates = mt5.copy_rates_from_pos(symbol, TIMEFRAME_MAP["M15"], 0, 30)
    if rates is None or len(rates) < 5:
        return False
    df  = pd.DataFrame(rates)
    tr  = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift(1)).abs(),
        (df["low"]  - df["close"].shift(1)).abs(),
    ], axis=1).max(axis=1)
    atr       = float(tr.ewm(alpha=1/14, adjust=False).mean().iloc[-1])
    threshold = atr * CONFIG["liquidity_sweep_atr_pct"]
    recent    = df.tail(CONFIG["fvg_reversal_bars"] + 2)
    if side == "BUY":
        swept     = (recent["low"]  < ar["low"]  - threshold).any()
        reversed_ = (recent["close"].iloc[-1] > ar["low"])
        return swept and reversed_
    else:
        swept     = (recent["high"] > ar["high"] + threshold).any()
        reversed_ = (recent["close"].iloc[-1] < ar["high"])
        return swept and reversed_


# =============================================================================
# S&D ZONE DETECTION — DBR / RBD / RBR / DBD
# =============================================================================
def _calc_atr(df: pd.DataFrame, period: int = 14) -> float:
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift(1)).abs(),
        (df["low"]  - df["close"].shift(1)).abs(),
    ], axis=1).max(axis=1)
    return float(tr.ewm(alpha=1/period, adjust=False).mean().iloc[-1])


def _has_fvg(df: pd.DataFrame, idx: int) -> bool:
    """True if candle at idx leaves a Fair Value Gap (imbalance)."""
    if idx < 1 or idx + 1 >= len(df):
        return False
    c1 = df.iloc[idx - 1]
    c3 = df.iloc[idx + 1]
    body = df.iloc[idx]
    if body["close"] > body["open"]:   # bullish displacement
        return float(c1["high"]) < float(c3["low"])
    else:                               # bearish displacement
        return float(c1["low"]) > float(c3["high"])


def _bos_confirmed(df: pd.DataFrame, idx: int, side: str, lookback: int) -> bool:
    """True if the displacement candle closes past the recent swing H/L."""
    start = max(0, idx - lookback)
    prior = df.iloc[start:idx]
    if len(prior) < 3:
        return False
    if side == "DEMAND":
        swing_high = float(prior["high"].max())
        return float(df.iloc[idx]["close"]) > swing_high
    else:
        swing_low = float(prior["low"].min())
        return float(df.iloc[idx]["close"]) < swing_low


def detect_sd_zones(symbol: str, tf_key: str = "H4") -> list:
    """
    Scan HTF bars for DBR/RBR (Demand) and RBD/DBD (Supply) patterns.
    Returns list of zone dicts with keys:
      type, top, bottom, base_top, base_bottom, ts, id, mitigated
    """
    tf    = TIMEFRAME_MAP[tf_key]
    rates = mt5.copy_rates_from_pos(symbol, tf, 0, 200)
    if rates is None or len(rates) < 20:
        return []
    df = pd.DataFrame(rates)
    df["time"]  = pd.to_datetime(df["time"], unit="s", utc=True)
    df["body"]  = (df["close"] - df["open"]).abs()
    atr         = _calc_atr(df)
    if atr < CONFIG.get("min_atr_value", 0.0001):
        return []

    disp_min  = atr * CONFIG["sd_displacement_atr_mult"]
    base_max  = atr * CONFIG["sd_base_body_atr_pct"]
    max_base  = CONFIG["sd_base_candles_max"]
    bos_look  = CONFIG["sd_bos_lookback"]
    expiry    = CONFIG["sd_zone_expiry_bars"]
    zones     = []
    n         = len(df)

    for i in range(2, n - max_base - 2):
        candle = df.iloc[i]
        body_i = float(candle["body"])

        # ── Displacement candle must be >= 2x ATR ──────────────────────────
        if body_i < disp_min:
            continue

        bullish_disp = float(candle["close"]) > float(candle["open"])

        # ── Find base: 1-3 small candles BEFORE displacement ───────────────
        for base_len in range(1, max_base + 1):
            base_start = i - base_len
            if base_start < 1:
                break
            base_slice = df.iloc[base_start:i]
            if not (base_slice["body"] <= base_max).all():
                break

            # candle before base (the "leg in")
            leg_in = df.iloc[base_start - 1]
            leg_in_bull = float(leg_in["close"]) > float(leg_in["open"])

            # ── Pattern classification ──────────────────────────────────────
            if bullish_disp:
                # DBR or RBR → Demand zone
                pattern = "RBR" if leg_in_bull else "DBR"
                zone_type = "DEMAND"
            else:
                # RBD or DBD → Supply zone
                pattern = "DBD" if not leg_in_bull else "RBD"
                zone_type = "SUPPLY"

            # ── FVG filter ──────────────────────────────────────────────────
            if not _has_fvg(df, i):
                break

            # ── BOS filter ──────────────────────────────────────────────────
            if not _bos_confirmed(df, i, zone_type, bos_look):
                break

            # ── Zone boundaries = base candles ──────────────────────────────
            base_high = float(base_slice["high"].max())
            base_low  = float(base_slice["low"].min())

            zone_id = f"{symbol}_{tf_key}_{zone_type}_{i}_{df.iloc[i]['time']}"
            zones.append({
                "id":          zone_id,
                "symbol":      symbol,
                "tf":          tf_key,
                "type":        zone_type,
                "pattern":     pattern,
                "top":         base_high,
                "bottom":      base_low,
                "ts":          df.iloc[i]["time"],
                "bar_idx":     i,
                "expiry_bar":  i + expiry,
                "mitigated":   False,
            })
            break   # found valid base, move to next displacement candle

    # ── Expire old zones ────────────────────────────────────────────────────
    current_bar = n - 1
    zones = [z for z in zones if z["expiry_bar"] >= current_bar]
    return zones


def refresh_sd_zones(symbol: str):
    """Merge newly detected zones into state, preserving mitigation flags."""
    mitigated = S.mitigated_zones.get(symbol, set())
    fresh = detect_sd_zones(symbol, CONFIG["htf"])
    # also scan H1 for finer zones
    fresh += detect_sd_zones(symbol, CONFIG["itf"])
    # remove already-mitigated
    fresh = [z for z in fresh if z["id"] not in mitigated]
    S.sd_zones[symbol] = fresh


def mark_mitigated(symbol: str, zone: dict):
    zone["mitigated"] = True
    S.mitigated_zones.setdefault(symbol, set()).add(zone["id"])
    S.sd_zones[symbol] = [z for z in S.sd_zones.get(symbol, [])
                          if z["id"] != zone["id"]]
    log.info(f"[zone] {symbol} {zone['type']} {zone['pattern']} mitigated "
             f"@ {zone['top']:.5f}-{zone['bottom']:.5f}")


def price_in_zone(price: float, zone: dict) -> bool:
    return zone["bottom"] <= price <= zone["top"]


def get_active_zone(symbol: str, price: float) -> dict | None:
    """Return the first fresh zone that price is currently inside."""
    for zone in S.sd_zones.get(symbol, []):
        if price_in_zone(price, zone):
            return zone
    return None


# =============================================================================
# CHoCH CONFIRMATION (5M / 1M)
# =============================================================================
def detect_choch(symbol: str, side: str) -> bool:
    """
    Change of Character on M5:
    BUY  → bearish structure broken: a candle closes above the last minor swing high
    SELL → bullish structure broken: a candle closes below the last minor swing low
    """
    tf    = TIMEFRAME_MAP[CONFIG["etf"]]
    rates = mt5.copy_rates_from_pos(symbol, tf, 0, 30)
    if rates is None or len(rates) < CONFIG["sd_choch_lookback"] + 2:
        return False
    df = pd.DataFrame(rates)
    lookback = CONFIG["sd_choch_lookback"]
    recent   = df.tail(lookback + 2).reset_index(drop=True)
    last     = recent.iloc[-1]

    if side == "BUY":
        # find the highest swing high in the lookback window (excluding last bar)
        swing_high = float(recent.iloc[:-1]["high"].max())
        return float(last["close"]) > swing_high
    else:
        swing_low = float(recent.iloc[:-1]["low"].min())
        return float(last["close"]) < swing_low


# =============================================================================
# RISK ENGINE — Dynamic Lot Sizing
# =============================================================================
def pip_value(symbol: str) -> float:
    """Return pip value in account currency per 1.0 lot."""
    info = mt5.symbol_info(symbol)
    if info is None:
        return 10.0
    # For JPY pairs 1 pip = 0.01; for others 0.0001
    pip_size = 0.01 if "JPY" in symbol else 0.0001
    # XAU/XAG use different pip sizes
    if symbol in ("XAUUSD", "XAGUSD"):
        pip_size = 0.01
    tick_val  = info.trade_tick_value
    tick_size = info.trade_tick_size
    if tick_size == 0:
        return 10.0
    return tick_val / tick_size * pip_size


def calc_lot_size(symbol: str, sl_price: float, entry_price: float) -> float:
    info = mt5.account_info()
    if info is None:
        return CONFIG["min_lot_size"]
    balance      = info.balance
    risk_amount  = balance * (CONFIG["risk_per_trade_pct"] / 100.0)
    pip_size     = 0.01 if ("JPY" in symbol or symbol in ("XAUUSD","XAGUSD")) else 0.0001
    sl_pips      = abs(entry_price - sl_price) / pip_size
    if sl_pips < 0.1:
        return CONFIG["min_lot_size"]
    pv           = pip_value(symbol)
    if pv <= 0:
        return CONFIG["min_lot_size"]
    lot = risk_amount / (sl_pips * pv)
    sym_info = mt5.symbol_info(symbol)
    if sym_info:
        lot = max(sym_info.volume_min,
                  min(sym_info.volume_max,
                      round(lot / sym_info.volume_step) * sym_info.volume_step))
    lot = max(CONFIG["min_lot_size"], min(CONFIG["max_lot_size"], lot))
    return round(lot, 2)


def spread_ok(symbol: str) -> bool:
    tick = mt5.symbol_info_tick(symbol)
    info = mt5.symbol_info(symbol)
    if tick is None or info is None:
        return False
    pip_size = 0.01 if ("JPY" in symbol or symbol in ("XAUUSD","XAGUSD")) else 0.0001
    spread_pips = (tick.ask - tick.bid) / pip_size
    cap = CONFIG["symbol_spread_pips"].get(symbol, CONFIG["max_spread_pips"])
    return spread_pips <= cap


def currency_exposure_ok(symbol: str) -> bool:
    """Enforce max 2 open trades per base currency."""
    base = symbol[:3]
    positions = mt5.positions_get()
    if not positions:
        return True
    count = sum(1 for p in positions
                if p.magic == CONFIG["magic_number"] and p.symbol[:3] == base)
    return count < CONFIG["max_open_per_currency"]


# =============================================================================
# ORDER EXECUTION
# =============================================================================
def place_limit_order(symbol: str, side: str, entry: float, sl: float,
                      tp1: float, tp2: float, lot: float, comment: str,
                      conn) -> int | None:
    order_type = mt5.ORDER_TYPE_BUY_LIMIT if side == "BUY" else mt5.ORDER_TYPE_SELL_LIMIT
    info = mt5.symbol_info(symbol)
    if info is None:
        return None
    digits = info.digits
    request = {
        "action":        mt5.TRADE_ACTION_PENDING,
        "symbol":        symbol,
        "volume":        lot,
        "type":          order_type,
        "price":         round(entry, digits),
        "sl":            round(sl,    digits),
        "tp":            round(tp2,   digits),
        "deviation":     CONFIG["max_slippage_points"],
        "magic":         CONFIG["magic_number"],
        "comment":       comment[:31],
        "type_time":     mt5.ORDER_TIME_GTC,
        "type_filling":  mt5.ORDER_FILLING_IOC,
    }
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        return None
    # retcode 10015: pending price too close to market — skip if within 1 pip
    pip_size = 0.01 if ("JPY" in symbol or symbol in ("XAUUSD", "XAGUSD")) else 0.0001
    market_price = tick.ask if side == "BUY" else tick.bid
    if abs(round(entry, info.digits) - market_price) < pip_size:
        log.info(f"[order] {symbol} {side} price {entry:.5f} too close to market "
                 f"{market_price:.5f} — skipping pending order")
        return None
    for attempt in range(CONFIG["max_order_retries"]):
        result = mt5.order_send(request)
        if result is None:
            log.warning(f"[order] {symbol} order_send returned None (attempt {attempt+1})")
            time.sleep(1)
            continue
        if result.retcode == mt5.TRADE_RETCODE_DONE:
            log.info(f"[order] {symbol} {side} limit placed @ {entry:.5f} "
                     f"SL={sl:.5f} TP={tp2:.5f} lot={lot}")
            log_trade(conn, symbol, side, lot, entry, sl, tp2,
                      result.retcode, comment, result.order)
            return result.order
        if result.retcode in NON_RETRYABLE:
            log.warning(f"[order] {symbol} non-retryable error {result.retcode}: "
                        f"{result.comment}")
            break
        log.warning(f"[order] {symbol} retcode={result.retcode} attempt {attempt+1}")
        time.sleep(1)
    return None


def modify_sltp(position, new_sl: float, new_tp: float = 0.0):
    info = mt5.symbol_info(position.symbol)
    if info is None:
        return
    d = info.digits
    mt5.order_send({
        "action":   mt5.TRADE_ACTION_SLTP,
        "position": position.ticket,
        "symbol":   position.symbol,
        "sl":       round(new_sl, d),
        "tp":       round(new_tp if new_tp else position.tp, d),
    })


# =============================================================================
# TRADE MANAGEMENT — Breakeven, TP1 partial, Trailing
# =============================================================================
def manage_open_trades(conn):
    positions = mt5.positions_get()
    if not positions:
        return
    for p in positions:
        if p.magic != CONFIG["magic_number"]:
            continue
        side      = "BUY" if p.type == mt5.POSITION_TYPE_BUY else "SELL"
        entry     = p.price_open
        sl        = p.sl
        if sl == 0:
            continue
        risk      = abs(entry - sl)
        if risk == 0:
            continue
        price     = p.price_current
        rr        = (price - entry) / risk if side == "BUY" else (entry - price) / risk

        # ── Breakeven at 1:1 ────────────────────────────────────────────────
        pip_size  = 0.01 if ("JPY" in p.symbol or p.symbol in ("XAUUSD","XAGUSD")) else 0.0001
        be_buffer = CONFIG["breakeven_buffer_pips"] * pip_size
        be_sl     = (entry + be_buffer) if side == "BUY" else (entry - be_buffer)
        if rr >= CONFIG["risk_reward_be"]:
            if (side == "BUY"  and p.sl < be_sl) or \
               (side == "SELL" and (p.sl == 0 or p.sl > be_sl)):
                modify_sltp(p, be_sl)
                log.info(f"[mgmt] {p.symbol} moved to breakeven")

        # ── TP1 partial close at 1:2 ─────────────────────────────────────
        if rr >= CONFIG["risk_reward_tp1"]:
            comment_tag = f"TP1_{p.ticket}"
            if comment_tag not in getattr(manage_open_trades, "_tp1_done", set()):
                close_partial(p, CONFIG["tp1_close_pct"], conn)
                if not hasattr(manage_open_trades, "_tp1_done"):
                    manage_open_trades._tp1_done = set()
                manage_open_trades._tp1_done.add(comment_tag)

        # ── Trailing stop after TP1 ──────────────────────────────────────
        if CONFIG["enable_trailing_stop"] and rr >= CONFIG["risk_reward_trail"]:
            atr_rates = mt5.copy_rates_from_pos(
                p.symbol, TIMEFRAME_MAP[CONFIG["etf"]], 0, 20)
            if atr_rates is not None and len(atr_rates) >= 14:
                atr_df  = pd.DataFrame(atr_rates)
                atr_val = _calc_atr(atr_df)
                trail   = atr_val * CONFIG["trailing_step_atr"]
                new_sl  = (price - trail) if side == "BUY" else (price + trail)
                if (side == "BUY"  and new_sl > p.sl) or \
                   (side == "SELL" and (p.sl == 0 or new_sl < p.sl)):
                    modify_sltp(p, new_sl)


def close_partial(position, pct: float, conn):
    close_vol = round(position.volume * pct, 2)
    info      = mt5.symbol_info(position.symbol)
    if info is None or close_vol < info.volume_min:
        return
    close_vol = max(info.volume_min,
                    round(close_vol / info.volume_step) * info.volume_step)
    order_type = mt5.ORDER_TYPE_SELL if position.type == mt5.POSITION_TYPE_BUY \
                 else mt5.ORDER_TYPE_BUY
    tick = mt5.symbol_info_tick(position.symbol)
    if tick is None:
        return
    price = tick.bid if order_type == mt5.ORDER_TYPE_SELL else tick.ask
    result = mt5.order_send({
        "action":       mt5.TRADE_ACTION_DEAL,
        "symbol":       position.symbol,
        "volume":       close_vol,
        "type":         order_type,
        "position":     position.ticket,
        "price":        price,
        "deviation":    CONFIG["max_slippage_points"],
        "magic":        CONFIG["magic_number"],
        "comment":      "TP1_partial",
        "type_filling": mt5.ORDER_FILLING_IOC,
    })
    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
        log.info(f"[mgmt] {position.symbol} partial close {close_vol} lots @ TP1")
        log_trade(conn, position.symbol,
                  "SELL" if order_type == mt5.ORDER_TYPE_SELL else "BUY",
                  close_vol, price, 0, 0, result.retcode, "TP1_partial", result.order)


# =============================================================================
# TELEGRAM EXECUTION SIGNAL
# =============================================================================
def send_execution_signal(symbol: str, side: str, entry: float, sl: float,
                          tp1: float, tp2: float, lot: float,
                          zone: dict, kz: str):
    pip_size = 0.01 if ("JPY" in symbol or symbol in ("XAUUSD","XAGUSD")) else 0.0001
    sl_pips  = round(abs(entry - sl) / pip_size, 1)
    eat_time = (datetime.now(timezone.utc) + timedelta(hours=3)).strftime("%H:%M EAT")
    arrow    = "📈" if side == "BUY" else "📉"
    msg = (
        f"⚡ EXECUTION SIGNAL ⚡\n"
        f"{arrow} Pair: {symbol} ({side} Limit Triggered)\n"
        f"🎯 Setup: {zone['tf']} {zone['type']} Zone ({zone['pattern']}) "
        f"+ 15M Liquidity Sweep + 5M CHoCH\n"
        f"🕐 Time: {eat_time} | Killzone: {kz}\n"
        f"📥 Entry:  {entry:.5f}\n"
        f"🛑 SL:     {sl:.5f}  ({sl_pips} pips)\n"
        f"🎯 TP1:    {tp1:.5f}  (1:2 RR — 50% close + Breakeven)\n"
        f"🎯 TP2:    {tp2:.5f}  (1:5 RR — Structural Target)\n"
        f"💰 Risk:   {CONFIG['risk_per_trade_pct']}% equity | Lot: {lot}"
    )
    telegram_send(msg)


# =============================================================================
# CORE SCAN — runs every poll cycle for one symbol
# =============================================================================
def scan_symbol(symbol: str, conn):
    # ── Gate 1: tradeable killzone ──────────────────────────────────────────
    if not in_tradeable_killzone():
        return
    kz = get_active_killzone()

    # ── Gate 2: news blackout ───────────────────────────────────────────────
    if news_blackout_active():
        return

    # ── Gate 3: spread cap ──────────────────────────────────────────────────
    if not spread_ok(symbol):
        log.debug(f"[scan] {symbol} spread too wide — skip")
        return

    # ── Gate 4: max open trades ─────────────────────────────────────────────
    positions = mt5.positions_get()
    open_count = sum(1 for p in (positions or [])
                     if p.magic == CONFIG["magic_number"])
    if open_count >= CONFIG["max_open_trades"]:
        return

    # ── Gate 5: one position per symbol ─────────────────────────────────────
    sym_count = sum(1 for p in (positions or [])
                    if p.magic == CONFIG["magic_number"] and p.symbol == symbol)
    if sym_count >= CONFIG["max_positions_per_symbol"]:
        return

    # ── Gate 6: currency correlation ────────────────────────────────────────
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        return
    price = (tick.bid + tick.ask) / 2.0

    # ── Step 1: refresh HTF S&D zones ───────────────────────────────────────
    refresh_sd_zones(symbol)
    zone = get_active_zone(symbol, price)
    if zone is None:
        return

    side = "BUY" if zone["type"] == "DEMAND" else "SELL"

    # ── Step 2: 15M liquidity sweep ─────────────────────────────────────────
    if not liquidity_swept(symbol, side):
        log.debug(f"[scan] {symbol} {side} zone hit but no liquidity sweep")
        return

    # ── Step 3: 5M CHoCH confirmation ───────────────────────────────────────
    if not detect_choch(symbol, side):
        log.debug(f"[scan] {symbol} {side} sweep confirmed, waiting CHoCH")
        return

    # ── Gate 6: currency exposure ────────────────────────────────────────────
    if not currency_exposure_ok(symbol):
        log.info(f"[scan] {symbol} currency exposure limit reached")
        return

    # ── Compute entry, SL, TP ────────────────────────────────────────────────
    info     = mt5.symbol_info(symbol)
    pip_size = 0.01 if ("JPY" in symbol or symbol in ("XAUUSD","XAGUSD")) else 0.0001
    sl_buf   = 1.5 * pip_size   # 1.5 pips below/above zone wick

    if side == "BUY":
        entry = zone["top"]
        sl    = zone["bottom"] - sl_buf
        risk  = abs(entry - sl)
        tp1   = entry + risk * CONFIG["risk_reward_tp1"]
        tp2   = entry + risk * CONFIG["risk_reward_tp2"]
    else:
        entry = zone["bottom"]
        sl    = zone["top"] + sl_buf
        risk  = abs(entry - sl)
        tp1   = entry - risk * CONFIG["risk_reward_tp1"]
        tp2   = entry - risk * CONFIG["risk_reward_tp2"]

    lot = calc_lot_size(symbol, sl, entry)

    # ── Fire limit order ─────────────────────────────────────────────────────
    comment  = f"SD_{zone['pattern']}_{kz[:2]}"
    order_id = place_limit_order(symbol, side, entry, sl, tp1, tp2, lot,
                                 comment, conn)
    if order_id is None:
        return

    # ── Mark zone mitigated (first touch rule) ───────────────────────────────
    mark_mitigated(symbol, zone)

    # ── Log signal ───────────────────────────────────────────────────────────
    atr_rates = mt5.copy_rates_from_pos(symbol, TIMEFRAME_MAP[CONFIG["htf"]], 0, 20)
    atr_val   = _calc_atr(pd.DataFrame(atr_rates)) if atr_rates is not None else 0.0
    log_signal(conn, symbol, side, f"SD_{zone['pattern']}", entry, sl, tp1, tp2,
               0.0, 0.0, zone["top"], zone["bottom"])

    # ── Telegram notification ────────────────────────────────────────────────
    send_execution_signal(symbol, side, entry, sl, tp1, tp2, lot, zone, kz)
    log.info(f"[signal] {symbol} {side} {zone['pattern']} entry={entry:.5f} "
             f"sl={sl:.5f} tp1={tp1:.5f} tp2={tp2:.5f} lot={lot}")


# =============================================================================
# HEARTBEAT & DAILY SUMMARY
# =============================================================================
def send_heartbeat():
    now = datetime.now(timezone.utc)
    if (S.last_heartbeat is not None and
            (now - S.last_heartbeat).total_seconds() <
            CONFIG["telegram_heartbeat_hours"] * 3600):
        return
    S.last_heartbeat = now
    info     = mt5.account_info()
    kz       = get_active_killzone() or "None"
    sessions = ", ".join(get_current_sessions()) or "None"
    eat      = (now + timedelta(hours=3)).strftime("%H:%M EAT")
    zones_total = sum(len(v) for v in S.sd_zones.values())
    positions   = mt5.positions_get() or []
    open_trades = sum(1 for p in positions if p.magic == CONFIG["magic_number"])
    bal  = f"{info.balance:.2f}" if info else "N/A"
    eq   = f"{info.equity:.2f}"  if info else "N/A"
    telegram_send(
        f"💓 KORIRBOT HEARTBEAT\n"
        f"🕐 {eat}\n"
        f"⚡ Killzone: {kz} | Sessions: {sessions}\n"
        f"💰 Balance: {bal} | Equity: {eq}\n"
        f"📊 Open Trades: {open_trades} | Fresh Zones: {zones_total}\n"
        f"🔍 Scanning: {len(CONFIG['symbols'])} pairs"
    )


def send_daily_summary(conn):
    now = datetime.now(timezone.utc)
    if now.hour != CONFIG["daily_summary_hour_utc"]:
        return
    if (S.last_daily_summary is not None and
            (now - S.last_daily_summary).total_seconds() < 3600):
        return
    S.last_daily_summary = now
    update_realized_pnl(conn)
    row  = get_or_init_daily_row(conn)
    info = mt5.account_info()
    pnl  = row["realized_pnl"] if row else 0.0
    bal  = info.balance if info else 0.0
    eat  = (now + timedelta(hours=3)).strftime("%d %b %Y")
    telegram_send(
        f"📋 DAILY SUMMARY — {eat}\n"
        f"💰 Realized P&L: {pnl:+.2f}\n"
        f"💼 Balance: {bal:.2f}\n"
        f"🔒 Circuit breaker: {'TRIGGERED' if row and row['halted'] else 'OK'}"
    )


# =============================================================================
# MT5 INITIALISATION
# =============================================================================
def _launch_mt5_terminal(path: str) -> bool:
    """Launch MT5 terminal process and wait for it to be ready."""
    import subprocess
    if not os.path.exists(path):
        log.error(f"[mt5] terminal not found at: {path}")
        return False
    log.info(f"[mt5] launching terminal: {path}")
    subprocess.Popen([path])
    for i in range(30):  # wait up to 30s
        time.sleep(1)
        if mt5.initialize(path=path):
            mt5.shutdown()
            return True
        log.info(f"[mt5] waiting for terminal to start... ({i+1}/30)")
    return False


def _enable_all_symbols():
    """Add all configured symbols to Market Watch."""
    added = []
    for sym in CONFIG["symbols"]:
        info = mt5.symbol_info(sym)
        if info is None:
            continue
        if not info.visible:
            mt5.symbol_select(sym, True)
            added.append(sym)
    if added:
        log.info(f"[mt5] added to Market Watch: {', '.join(added)}")


def init_mt5() -> bool:
    cfg = CONFIG["mt5"]
    # Try to initialize — if terminal not running, launch it
    kwargs = {"path": cfg["path"]} if cfg["path"] else {}
    if not mt5.initialize(**kwargs):
        err = mt5.last_error()
        log.warning(f"[mt5] initialize failed ({err}) — attempting to launch terminal")
        if cfg["path"] and _launch_mt5_terminal(cfg["path"]):
            if not mt5.initialize(path=cfg["path"]):
                log.error(f"[mt5] still failed after launch: {mt5.last_error()}")
                return False
        else:
            log.error("[mt5] could not launch terminal — open MT5 manually and retry")
            return False
    if cfg["login"] and cfg["password"] and cfg["server"]:
        if not mt5.login(cfg["login"], password=cfg["password"],
                         server=cfg["server"]):
            log.error(f"[mt5] login failed: {mt5.last_error()}")
            mt5.shutdown()
            return False
    # Auto-add all symbols to Market Watch
    _enable_all_symbols()
    info = mt5.account_info()
    if info:
        log.info(f"[mt5] connected — {info.server} | #{info.login} | balance={info.balance:.2f}")
    return True


# =============================================================================
# MAIN LOOP
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description="KorirBot S&D Engine")
    parser.add_argument("--once", action="store_true",
                        help="Run one scan cycle then exit (for testing)")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("KORIRBOT — Supply & Demand Confluence Engine STARTING")
    log.info("=" * 60)

    if not init_mt5():
        sys.exit(1)

    conn = db_connect()

    # Startup: announce upcoming news
    send_upcoming_events_alert(CONFIG["startup_events_lookahead_minutes"])

    telegram_send(
        "🚀 KORIRBOT ONLINE\n"
        "📊 Strategy: Supply & Demand Confluence Engine\n"
        f"🔍 Scanning {len(CONFIG['symbols'])} pairs\n"
        "⚡ Killzones: London 10:00-13:00 EAT | NY 15:00-18:00 EAT\n"
        "🛡️ Risk: 1% per trade | 3% daily circuit breaker"
    )

    try:
        while True:
            try:
                # ── Session / killzone change alerts ────────────────────────
                check_session_change()

                # ── News alert ───────────────────────────────────────────────
                send_upcoming_events_alert()

                # ── Circuit breaker check ────────────────────────────────────
                if trading_halted_for_today(conn):
                    log.info("[main] trading halted for today — sleeping")
                    time.sleep(CONFIG["poll_seconds"])
                    continue

                # ── Asian range mode: no trades ──────────────────────────────
                kz = get_active_killzone()
                if kz == "Asian Range":
                    # still update ranges so they're ready for London open
                    for sym in CONFIG["symbols"]:
                        update_asian_range(sym)
                    time.sleep(CONFIG["poll_seconds"])
                    continue

                # ── Manage existing trades ───────────────────────────────────
                manage_open_trades(conn)
                update_realized_pnl(conn)

                # ── Scan all symbols ─────────────────────────────────────────
                if in_tradeable_killzone():
                    for sym in CONFIG["symbols"]:
                        try:
                            scan_symbol(sym, conn)
                        except Exception as e:
                            log.error(f"[scan] {sym} error: {e}\n"
                                      f"{traceback.format_exc()}")

                # ── Heartbeat & daily summary ────────────────────────────────
                send_heartbeat()
                send_daily_summary(conn)

            except Exception as e:
                log.error(f"[main] loop error: {e}\n{traceback.format_exc()}")

            if args.once:
                break
            time.sleep(CONFIG["poll_seconds"])

    except KeyboardInterrupt:
        log.info("[main] interrupted by user")
    finally:
        telegram_send("🔴 KORIRBOT OFFLINE — manual shutdown")
        mt5.shutdown()
        conn.close()
        log.info("[main] shutdown complete")


if __name__ == "__main__":
    main()
