"""
bot.py — THE PERFECT FOREX BOT: UNIFIED BLUEPRINT
===================================================
STRATEGY: ICT/SMC Confluence Engine (3-in-1)
  Step 1 — Trend Bias    : 4H/D1 BOS/MSS structure (Higher TF SMC)
  Step 2 — Liquidity     : Asian Range sweep filter (London/NY killzones)
  Step 3 — Entry Trigger : Fair Value Gap (FVG) on 5M/15M + limit at 50% EQ

MODULES:
  1. Time/Session/News Architecture (UTC-only, 3 killzones, 30/30 news rule)
  2. Confluence Engine (BOS + Liquidity Sweep + FVG limit order)
  3. Risk & Safeguards (dynamic sizing, spread guard, 3% daily circuit breaker,
     breakeven at 1:1, trailing at 1:2, target 1:3–1:5)
  4. Telegram Interface (session alerts, signal format, heartbeat, reports)

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
    # Timeframes used by the confluence engine
    "htf": "H4",        # Higher timeframe for BOS/MSS bias
    "mtf": "M15",       # Mid timeframe for liquidity sweep detection
    "etf": "M5",        # Entry timeframe for FVG detection
    "exec_tf": "H1",    # Execution / indicator timeframe
    # Indicators
    "ema_fast": 50, "ema_slow": 200,
    "macd_fast": 12, "macd_slow": 26, "macd_signal": 9,
    "rsi_period": 14,
    "bb_period": 20, "bb_std": 2.0,
    "adx_period": 14, "adx_threshold": 20,
    "require_adx_rising": True,
    "atr_period": 14,
    # Risk
    "atr_sl_multiplier": 1.5,
    "news_atr_sl_multiplier": 2.5,
    "risk_reward_ratio": 3.0,       # blueprint target 1:3
    "risk_reward_be": 1.0,          # move to BE at 1:1
    "risk_reward_trail": 2.0,       # start trailing at 1:2
    "risk_per_trade_pct": 1.0,
    "max_daily_loss_pct": 3.0,      # blueprint: 3% daily circuit breaker
    "drawdown_warning_pct": 2.0,
    "max_open_trades": 6,
    "max_open_per_currency": 2,
    "max_positions_per_symbol": 1,
    "magic_number": 123456,
    "poll_seconds": 30,             # faster poll for FVG entries
    "max_spread_pips": 2.0,         # default for majors
    "symbol_spread_pips": {          # per-symbol spread overrides
        "XAUUSD": 5.0, "XAGUSD": 8.0,
        "GBPJPY": 3.0, "EURJPY": 3.0, "USDJPY": 3.0,
        "AUDJPY": 3.0, "CADJPY": 3.0, "NZDJPY": 3.0, "CHFJPY": 3.0,
        "GBPNZD": 4.0, "EURNZD": 4.0, "AUDNZD": 3.5,
    },
    "cooldown_minutes_after_loss": 15,
    "volume_lookback": 20,
    "volume_threshold": 0.0,
    "enable_trailing_stop": True,
    "trailing_step_atr": 0.5,
    "breakeven_buffer_pips": 2,     # entry + 2 pips commission buffer at BE
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
    "news_blackout_minutes": 30,        # blueprint: 30 min before
    "news_resume_minutes": 30,          # blueprint: 30 min after
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
    # FVG settings
    "fvg_min_size_atr_pct": 0.3,    # FVG must be >= 30% of ATR to be valid
    "fvg_expiry_bars": 10,          # FVG limit order expires after N bars
    "fvg_reversal_bars": 3,         # bars to confirm sweep reversal back inside range
    "calendar_fallback_events": [],  # manual fallback if FF feed is down
    "startup_events_lookahead_minutes": 120,  # alert window on bot start
    # Asian range
    "asian_range_start_hour": 22,   # 22:00 UTC previous day
    "asian_range_end_hour": 6,      # 06:00 UTC today
    "liquidity_sweep_atr_pct": 0.5, # sweep must exceed range by N * ATR
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
        "path": os.getenv("MT5_PATH", r"C:\Program Files\FBS MetaTrader 5\terminal64.exe"),
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

# =============================================================================
# KILLZONES (UTC) — blueprint Module 1
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

USD_POSITIVE = {"USDJPY", "USDCAD", "USDCHF"}

# =============================================================================
# IN-MEMORY STATE — single container eliminates bare globals
# =============================================================================
class _State:
    cooldown_until:      dict  = None
    pyramid_applied:     dict  = None
    last_session:        set   = None
    last_killzone:       str   = None
    last_heartbeat             = None
    last_daily_summary         = None
    last_weekly_report         = None
    drawdown_warned:     bool  = False
    calendar_cache:      list  = None
    calendar_fetched_at        = None
    predicted_events:    set   = None
    asian_ranges:        dict  = None
    active_fvgs:         dict  = None
    last_events_alert_keys: set = None

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
        self.active_fvgs            = {}
        self.last_events_alert_keys = set()

S = _State()

# =============================================================================
# TELEGRAM
# =============================================================================
def telegram_send(text: str) -> None:
    tg = CONFIG["telegram"]
    if not tg["enabled"] or not tg["bot_token"] or not tg["chat_id"]:
        return
    try:
        url = f"https://api.telegram.org/bot{tg['bot_token']}/sendMessage"
        requests.post(url, data={"chat_id": tg["chat_id"], "text": text}, timeout=10)
    except Exception as e:
        log.warning(f"[telegram] {e}")


# =============================================================================
# SQLITE
# =============================================================================
def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(CONFIG["db_path"])
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, symbol TEXT, side TEXT,
            strategy TEXT, entry_price REAL, sl REAL, tp1 REAL, tp2 REAL,
            adx REAL, rsi REAL, fvg_high REAL, fvg_low REAL)""")
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
        # Schema migrations — add missing columns to existing databases
        existing = {row[1] for row in conn.execute("PRAGMA table_info(signals)")}
        for col, typedef in [
            ("strategy",  "TEXT"),
            ("tp1",       "REAL"),
            ("tp2",       "REAL"),
            ("adx",       "REAL"),
            ("rsi",       "REAL"),
            ("fvg_high",  "REAL"),
            ("fvg_low",   "REAL"),
        ]:
            if col not in existing:
                conn.execute(f"ALTER TABLE signals ADD COLUMN {col} {typedef}")
        conn.commit()
    except Exception:
        conn.close()
        raise
    return conn


def log_signal(conn, symbol, side, strategy, entry, sl, tp1, tp2, adx, rsi,
               fvg_high=None, fvg_low=None):
    conn.execute(
        """INSERT INTO signals
           (ts,symbol,side,strategy,entry_price,sl,tp1,tp2,adx,rsi,fvg_high,fvg_low)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (datetime.now(timezone.utc).isoformat(), symbol, side, strategy,
         entry, sl, tp1, tp2, adx, rsi, fvg_high, fvg_low))
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
    """Sync today's realized P&L from MT5 closed deals into daily_pnl table."""
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
# DAILY LOSS / DRAWDOWN — blueprint: 3% circuit breaker
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
    row  = get_or_init_daily_row(conn)
    info = mt5.account_info()
    if info is None or row["start_balance"] == 0:
        return False
    # Blueprint: realized + unrealized losses combined
    pct = (info.equity - row["start_balance"]) / row["start_balance"] * 100.0

    if not S.drawdown_warned and pct <= -CONFIG["drawdown_warning_pct"]:
        telegram_send(
            f"⚠️ DRAWDOWN WARNING: {pct:.2f}% today (realized + unrealized)\n"
            f"Circuit breaker triggers at -{CONFIG['max_daily_loss_pct']}%"
        )
        S.drawdown_warned = True

    if pct <= -CONFIG["max_daily_loss_pct"]:
        if not row["halted"]:
            msg = (f"🛑 CIRCUIT BREAKER FIRED: {pct:.2f}% daily loss\n"
                   f"Realized + unrealized losses exceeded -{CONFIG['max_daily_loss_pct']}%\n"
                   f"All entries locked until 00:00 UTC.")
            log.warning(msg)
            telegram_send(msg)
            _close_all_positions_to_breakeven(conn)
            today_str = datetime.now(timezone.utc).date().isoformat()
            conn.execute("UPDATE daily_pnl SET halted=1 WHERE date=?", (today_str,))
            conn.commit()
        return True
    return False


def _close_all_positions_to_breakeven(conn):
    """Move all open positions to breakeven when circuit breaker fires."""
    positions = mt5.positions_get()
    if not positions:
        return
    for p in positions:
        if p.magic != CONFIG["magic_number"]:
            continue
        modify_sltp(p, p.price_open)
        log.info(f"[circuit] {p.symbol} moved to breakeven")


# =============================================================================
# FOREX CALENDAR — blueprint: 30/30 rule
# =============================================================================
def fetch_forex_calendar() -> list:
    now = datetime.now(timezone.utc)
    if (S.calendar_fetched_at is not None and
            (now - S.calendar_fetched_at).total_seconds() <
            CONFIG["calendar_fetch_interval_minutes"] * 60):
        return S.calendar_cache
    # Respect a minimum retry gap to avoid 429 rate-limit errors
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
                log.debug(f"[calendar] skipping event parse error: {parse_err}")
                continue
        S.calendar_cache      = events
        S.calendar_fetched_at = now
        log.info(f"[calendar] {len(events)} high-impact events this week")
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 429:
            log.warning("[calendar] rate-limited (429) — using cached data, retry in 60s")
            S.calendar_fetched_at = now  # reset timer to prevent hammering
        else:
            log.warning(f"[calendar] fetch failed: {e}")
        if not S.calendar_cache and CONFIG.get("calendar_fallback_events"):
            S.calendar_cache = CONFIG["calendar_fallback_events"]
            log.warning("[calendar] using fallback manual events")
            telegram_send("⚠️ ForexFactory feed down — using manual fallback calendar.")
    except Exception as e:
        log.warning(f"[calendar] fetch failed: {e}")
        if not S.calendar_cache and CONFIG.get("calendar_fallback_events"):
            S.calendar_cache = CONFIG["calendar_fallback_events"]
            log.warning("[calendar] using fallback manual events")
            telegram_send("⚠️ ForexFactory feed down — using manual fallback calendar.")
    return S.calendar_cache


def get_upcoming_events(lookahead_minutes: int = 60) -> list:
    now = datetime.now(timezone.utc)
    return [(dt, t, c) for dt, t, c, _ in fetch_forex_calendar()
            if timedelta(0) <= (dt - now) <= timedelta(minutes=lookahead_minutes)]


def news_blackout_active() -> bool:
    """True during 30 min BEFORE any high-impact event."""
    now     = datetime.now(timezone.utc)
    blackout = timedelta(minutes=CONFIG["news_blackout_minutes"])
    for dt, _, _, _ in fetch_forex_calendar():
        if -blackout <= (now - dt) < timedelta(0):
            return True
    return False


def news_resume_window_active():
    """Returns (event_dt, title, country) during 30 min AFTER event, else None."""
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
    lines = [f"⚠️ HIGH-IMPACT NEWS IN {lookahead} MIN — PREPARING BLACKOUT", ""]
    for dt, title, country in events:
        eat = (dt + timedelta(hours=3)).strftime("%H:%M EAT")
        lines.append(f"🔔 {title} ({country}) — {dt.strftime('%H:%M UTC')} / {eat}")
    lines += ["", "⏸ Entries paused 30 min before & 30 min after each event."]
    telegram_send("\n".join(lines))


# =============================================================================
# SESSION & KILLZONE TRACKER — blueprint Module 1
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
                f"Asian Range mapped. Scanning for liquidity sweeps."
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
            info = KILLZONES[kz]
            status = "ACTIVE — scanning for setups" if info["trade"] else "MONITORING only"
            telegram_send(
                f"{info['emoji']} {kz.upper()} KILLZONE\n"
                f"🕐 {now_str} / {eat_str}\n"
                f"⚡ Status: {status}\n"
                f"📊 Tracking: {len(CONFIG['symbols'])} pairs"
            )
        S.last_killzone = kz


# =============================================================================
# ASIAN RANGE — blueprint: map High/Low 22:00–06:00 UTC
# =============================================================================
def update_asian_range(symbol: str):
    """Fetch M15 bars covering the Asian session 22:00–06:00 UTC and store High/Low."""
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

    # Asian session: 22:00 UTC previous day to 06:00 UTC today
    from datetime import time as dtime
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
    """
    True if price swept the Asian High/Low AND closed back inside the range
    (reversal confirmation) within the last fvg_reversal_bars bars.
    This is the key ICT concept: sweep + rejection, not just a touch.
    """
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
    reversal_bars = CONFIG["fvg_reversal_bars"]

    # Check last N bars for sweep + close back inside range
    recent = df.tail(reversal_bars + 2)
    if side == "BUY":
        # Bullish: wick swept below Asian Low, but candle CLOSED back above it
        swept   = (recent["low"]   < ar["low"] - threshold).any()
        reversed_ = (recent["close"].iloc[-1] > ar["low"])  # closed back inside
        return swept and reversed_
    else:
        # Bearish: wick swept above Asian High, but candle CLOSED back below it
        swept   = (recent["high"]  > ar["high"] + threshold).any()
        reversed_ = (recent["close"].iloc[-1] < ar["high"]) # closed back inside
        return swept and reversed_


# =============================================================================
# FAIR VALUE GAP (FVG) SCANNER — blueprint Module 2 Step 3
# =============================================================================
def scan_fvg(symbol: str, tf_name: str = "M5") -> list:
    """
    Scan for Fair Value Gaps on the given timeframe.
    FVG (bullish): candle[i-2].high < candle[i].low  (gap between c1 high and c3 low)
    FVG (bearish): candle[i-2].low  > candle[i].high (gap between c1 low and c3 high)
    Returns list of dicts: {side, top, bottom, mid, bar_index, atr}
    """
    tf    = TIMEFRAME_MAP.get(tf_name, mt5.TIMEFRAME_M5)
    rates = mt5.copy_rates_from_pos(symbol, tf, 0, 50)
    if rates is None or len(rates) < 5:
        return []

    df = pd.DataFrame(rates)
    df["time"] = pd.to_datetime(df["time"], unit="s")
    tr  = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift(1)).abs(),
        (df["low"]  - df["close"].shift(1)).abs(),
    ], axis=1).max(axis=1)
    atr = float(tr.ewm(alpha=1/14, adjust=False).mean().iloc[-1])
    min_size = atr * CONFIG["fvg_min_size_atr_pct"]

    fvgs = []
    for i in range(2, len(df) - 1):
        c1 = df.iloc[i - 2]
        c3 = df.iloc[i]
        # Bullish FVG: c1 high < c3 low
        if c3["low"] > c1["high"] and (c3["low"] - c1["high"]) >= min_size:
            fvgs.append({
                "side":      "BUY",
                "top":       float(c3["low"]),
                "bottom":    float(c1["high"]),
                "mid":       float((c3["low"] + c1["high"]) / 2),
                "bar_index": i,
                "atr":       atr,
                "time":      c3["time"],
            })
        # Bearish FVG: c1 low > c3 high
        elif c1["low"] > c3["high"] and (c1["low"] - c3["high"]) >= min_size:
            fvgs.append({
                "side":      "SELL",
                "top":       float(c1["low"]),
                "bottom":    float(c3["high"]),
                "mid":       float((c1["low"] + c3["high"]) / 2),
                "bar_index": i,
                "atr":       atr,
                "time":      c3["time"],
            })

    # Return only the most recent FVG per side
    result = []
    seen   = set()
    for fvg in reversed(fvgs):
        if fvg["side"] not in seen:
            result.append(fvg)
            seen.add(fvg["side"])
    return result


# =============================================================================
# INDICATORS
# =============================================================================
def fetch_rates(symbol: str, tf_name: str = "H1", bars: int = 400,
                min_bars: int = None):
    tf = TIMEFRAME_MAP.get(tf_name, mt5.TIMEFRAME_H1)
    if min_bars is None:
        min_bars = CONFIG["ema_slow"] + CONFIG["adx_period"] + 5
    rates = mt5.copy_rates_from_pos(symbol, tf, 0, bars)
    if rates is None or len(rates) < min_bars:
        return None
    df = pd.DataFrame(rates)
    df["time"] = pd.to_datetime(df["time"], unit="s")
    return df


def _compute_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain  = delta.clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(alpha=1/period, adjust=False).mean()
    rs    = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df    = df.copy()
    close = df["close"]
    high  = df["high"]
    low   = df["low"]

    df["ema_fast"] = close.ewm(span=CONFIG["ema_fast"], adjust=False).mean()
    df["ema_slow"] = close.ewm(span=CONFIG["ema_slow"], adjust=False).mean()

    prev_close = close.shift(1)
    tr = pd.concat([high-low, (high-prev_close).abs(),
                    (low-prev_close).abs()], axis=1).max(axis=1)
    df["atr"] = tr.ewm(alpha=1/CONFIG["atr_period"], adjust=False).mean()

    up  = high.diff()
    dn  = -low.diff()
    pdm = np.where((up > dn) & (up > 0), up, 0.0)
    mdm = np.where((dn > up) & (dn > 0), dn, 0.0)
    atr_di = tr.ewm(alpha=1/CONFIG["adx_period"], adjust=False).mean()
    pdi = 100 * pd.Series(pdm, index=df.index).ewm(
        alpha=1/CONFIG["adx_period"], adjust=False).mean() / atr_di
    mdi = 100 * pd.Series(mdm, index=df.index).ewm(
        alpha=1/CONFIG["adx_period"], adjust=False).mean() / atr_di
    dx  = (pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan) * 100
    df["adx"]      = dx.ewm(alpha=1/CONFIG["adx_period"], adjust=False).mean()
    df["plus_di"]  = pdi
    df["minus_di"] = mdi

    df["rsi"] = _compute_rsi(close, CONFIG["rsi_period"])

    ema_f = close.ewm(span=CONFIG["macd_fast"], adjust=False).mean()
    ema_s = close.ewm(span=CONFIG["macd_slow"], adjust=False).mean()
    df["macd"]        = ema_f - ema_s
    df["macd_signal"] = df["macd"].ewm(span=CONFIG["macd_signal"], adjust=False).mean()
    df["macd_hist"]   = df["macd"] - df["macd_signal"]

    df["bb_mid"]   = close.rolling(CONFIG["bb_period"]).mean()
    bb_std         = close.rolling(CONFIG["bb_period"]).std()
    df["bb_upper"] = df["bb_mid"] + CONFIG["bb_std"] * bb_std
    df["bb_lower"] = df["bb_mid"] - CONFIG["bb_std"] * bb_std
    df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / df["bb_mid"]
    df["vol_avg"]  = df["tick_volume"].rolling(CONFIG["volume_lookback"]).mean()
    return df


def min_atr_for(symbol: str) -> float:
    return CONFIG["symbol_configs"].get(symbol, {}).get(
        "min_atr_value", CONFIG["min_atr_value"])


def pip_size(symbol: str) -> float:
    info = mt5.symbol_info(symbol)
    if info is None:
        return 0.0001
    return info.point * (10 if info.digits in (3, 5) else 1)


def spread_pips(symbol: str) -> float:
    info = mt5.symbol_info(symbol)
    if info is None:
        return 999.0
    return info.spread * info.point / pip_size(symbol)


def max_spread_for(symbol: str) -> float:
    """Per-symbol spread threshold — metals and exotics have wider natural spreads."""
    return CONFIG["symbol_spread_pips"].get(symbol, CONFIG["max_spread_pips"])

# =============================================================================
# MODULE 2 STEP 1 — HTF BOS/MSS BIAS
# =============================================================================
def get_htf_bias(symbol: str) -> str | None:
    """
    Scan 4H chart for Break of Structure (BOS) or Market Structure Shift (MSS).
    Returns 'BUY', 'SELL', or None if no clear bias.
    """
    df = fetch_rates(symbol, tf_name=CONFIG["htf"], bars=100, min_bars=30)
    if df is None:
        return None
    df   = compute_indicators(df)
    last = df.iloc[-2]
    prev = df.iloc[-3]

    # EMA alignment as primary trend filter
    bull_ema = last["ema_fast"] > last["ema_slow"]

    # BOS: new swing high/low breaking previous structure
    swing_highs = df["high"].iloc[-20:-2]
    swing_lows  = df["low"].iloc[-20:-2]
    bos_bull = df["high"].iloc[-2] > swing_highs.max()   # broke above structure
    bos_bear = df["low"].iloc[-2]  < swing_lows.min()    # broke below structure

    # MSS: EMA crossover as structure shift proxy
    mss_bull = (last["ema_fast"] > last["ema_slow"] and
                prev["ema_fast"] <= prev["ema_slow"])
    mss_bear = (last["ema_fast"] < last["ema_slow"] and
                prev["ema_fast"] >= prev["ema_slow"])

    if (bos_bull or mss_bull) and bull_ema:
        return "BUY"
    if (bos_bear or mss_bear) and not bull_ema:
        return "SELL"
    # Fallback: pure EMA alignment
    return "BUY" if bull_ema else "SELL"


# =============================================================================
# MODULE 2 — FULL CONFLUENCE ENGINE
# =============================================================================
def strategy_confluence_engine(symbol: str, df_exec: pd.DataFrame):
    """
    3-step ICT/SMC Confluence Engine.
    Returns (side, fvg, reason) or (None, None, reason).

    Step 1: HTF BOS/MSS bias
    Step 2: Liquidity sweep of Asian range during killzone
    Step 3: FVG on M5 — place limit at 50% equilibrium
    """
    if len(df_exec) < 20:
        return None, None, "insufficient_bars"

    last = df_exec.iloc[-2]
    prev = df_exec.iloc[-3]

    # Gate: ADX
    if last["adx"] < CONFIG["adx_threshold"]:
        return None, None, "adx_below_threshold"
    if CONFIG["require_adx_rising"] and last["adx"] <= prev["adx"]:
        return None, None, "adx_not_rising"

    # Gate: ATR floor
    if last["atr"] < min_atr_for(symbol):
        return None, None, "atr_below_floor"

    # Gate: spread
    if spread_pips(symbol) > max_spread_for(symbol):
        return None, None, "spread_too_wide"

    # Step 1: HTF bias
    bias = get_htf_bias(symbol)
    if bias is None:
        return None, None, "no_htf_bias"

    # Step 2: Liquidity sweep (only required during killzones)
    if in_tradeable_killzone():
        swept = liquidity_swept(symbol, bias)
        if not swept:
            return None, None, "no_liquidity_sweep"

    # Step 3: FVG scan on M5
    fvgs = scan_fvg(symbol, tf_name=CONFIG["etf"])
    matching = [f for f in fvgs if f["side"] == bias]
    if not matching:
        # Fallback: check M15 FVG
        fvgs = scan_fvg(symbol, tf_name=CONFIG["mtf"])
        matching = [f for f in fvgs if f["side"] == bias]
    if not matching:
        return None, None, "no_fvg"

    fvg = matching[0]

    # Confirm with MACD + RSI on exec TF
    macd_ok = (last["macd_hist"] > 0) if bias == "BUY" else (last["macd_hist"] < 0)
    rsi_ok  = (40 <= last["rsi"] <= 70) if bias == "BUY" else (30 <= last["rsi"] <= 60)
    if not macd_ok:
        return None, None, "macd_no_confirm"
    if not rsi_ok:
        return None, None, "rsi_out_of_range"

    return bias, fvg, "ok"


# =============================================================================
# NEWS PREDICTION
# =============================================================================
def _fetch_multi_tf(symbol: str) -> dict | None:
    result = {}
    for tf_name in ("H1", "H4", "D1"):
        df = fetch_rates(symbol, tf_name=tf_name, bars=300, min_bars=50)
        if df is None:
            return None
        result[tf_name] = compute_indicators(df)
    return result


def _score_symbol(symbol: str, tfs: dict) -> tuple:
    is_usd_pos  = symbol in USD_POSITIVE
    votes_hot   = votes_soft = 0
    details     = []
    for tf_name, df in tfs.items():
        last = df.iloc[-2]
        w    = {"H1": 1, "H4": 2, "D1": 3}.get(tf_name, 1)
        ema_bull  = last["ema_fast"] > last["ema_slow"]
        macd_bull = last["macd_hist"] > 0
        rsi_bull  = last["rsi"] >= 50
        for bull, label in [(ema_bull, "EMA"), (macd_bull, "MACD"), (rsi_bull, "RSI")]:
            hot = (bull and is_usd_pos) or (not bull and not is_usd_pos)
            if hot: votes_hot  += w; details.append(f"{tf_name} {label}↑")
            else:   votes_soft += w; details.append(f"{tf_name} {label}↓")
        if last["adx"] >= 25:
            hot = (ema_bull and is_usd_pos) or (not ema_bull and not is_usd_pos)
            if hot: votes_hot  += w * 2
            else:   votes_soft += w * 2
    total = votes_hot + votes_soft
    if total == 0:
        return 0.5, 50, details
    score = votes_hot / total
    conf  = round(score * 100) if score >= 0.5 else round((1 - score) * 100)
    return score, conf, details


def predict_news_event(event_title: str, event_country: str):
    now_utc = datetime.now(timezone.utc)
    now_eat = now_utc + timedelta(hours=3)
    pair_results = {}
    for symbol in CONFIG["news_symbols"]:
        tfs = _fetch_multi_tf(symbol)
        if tfs is None:
            continue
        score, conf, details = _score_symbol(symbol, tfs)
        atr  = tfs["H1"].iloc[-2]["atr"]
        tick = mt5.symbol_info_tick(symbol)
        side = ("BUY" if score >= 0.5 else "SELL") if symbol in USD_POSITIVE \
               else ("SELL" if score >= 0.5 else "BUY")
        pair_results[symbol] = (side, conf, details, atr, tick)

    if not pair_results:
        return

    low_conf = CONFIG["low_confidence_threshold"]
    lines    = [
        f"📰 NEWS PREDICTION: {event_title} ({event_country})",
        f"🕐 {now_utc.strftime('%H:%M UTC')} / {now_eat.strftime('%H:%M EAT')}",
        "━" * 42, "🎯 TRADE SETUPS", "━" * 42,
    ]
    for sym, (side, conf, details, atr, tick) in pair_results.items():
        if tick is None or atr <= 0:
            continue
        entry   = tick.ask if side == "BUY" else tick.bid
        sl_d    = atr * CONFIG["news_atr_sl_multiplier"]
        tp1     = entry + sl_d * 2 if side == "BUY" else entry - sl_d * 2
        tp2     = entry + sl_d * CONFIG["risk_reward_ratio"] if side == "BUY" \
                  else entry - sl_d * CONFIG["risk_reward_ratio"]
        sl      = entry - sl_d if side == "BUY" else entry + sl_d
        sl_pips = round(sl_d / pip_size(sym))
        arrow   = "📈 BUY  ⬆️" if side == "BUY" else "📉 SELL ⬇️"
        caution = "\n  ⚠️ Low confidence — reduce size" if conf < low_conf else ""
        lines.append(
            f"\n⚡ NEW SIGNAL EXECUTED ⚡\n"
            f"📈 Pair: {sym} ({arrow})\n"
            f"🎯 Strategy: News Confluence ({event_title})\n"
            f"📥 Entry Price: {entry:.5f}\n"
            f"🛑 Stop Loss: {sl:.5f} ({sl_pips} Pips)\n"
            f"🎯 Take Profit 1: {tp1:.5f} (1:2 RR - Move to BE)\n"
            f"🎯 Take Profit 2: {tp2:.5f} (1:{CONFIG['risk_reward_ratio']:.0f} RR - Target)\n"
            f"💰 Calculated Risk: {CONFIG['risk_per_trade_pct']:.2f}% (Exact Position Sized)\n"
            f"📊 Confidence: {conf}%  |  Basis: {' | '.join(details[:3])}{caution}"
        )
    lines += ["", "━" * 42,
              "✅ Wait for breakout candle to close before entering",
              "❌ Do NOT trade against the signal direction", "━" * 42]
    msg = "\n".join(lines)
    log.info("\n" + msg)
    telegram_send(msg)


# =============================================================================
# POSITION SIZING — blueprint dynamic equation
# =============================================================================
def calc_lot_size(symbol: str, sl_distance_price: float,
                  lot_multiplier: float = 1.0) -> float:
    info    = mt5.symbol_info(symbol)
    account = mt5.account_info()
    if info is None or account is None or sl_distance_price <= 0:
        return 0.0
    risk_amount  = account.balance * (CONFIG["risk_per_trade_pct"] / 100.0) * lot_multiplier
    tick_size    = info.trade_tick_size or info.point
    tick_value   = info.trade_tick_value
    if tick_size <= 0 or tick_value <= 0:
        return 0.0
    loss_per_lot = (sl_distance_price / tick_size) * tick_value
    if loss_per_lot <= 0:
        return 0.0
    lots = risk_amount / loss_per_lot
    lots = max(CONFIG["min_lot_size"], min(CONFIG["max_lot_size"], lots))
    step = info.volume_step or 0.01
    lots = round(round(lots / step) * step, 2)
    return max(lots, info.volume_min or CONFIG["min_lot_size"])


# =============================================================================
# ORDER EXECUTION
# =============================================================================
def get_filling_mode(symbol: str) -> int:
    info = mt5.symbol_info(symbol)
    if info is None:
        return mt5.ORDER_FILLING_IOC
    flags = info.filling_mode
    if flags & 1:
        return mt5.ORDER_FILLING_FOK
    if flags & 2:
        return mt5.ORDER_FILLING_IOC
    return mt5.ORDER_FILLING_RETURN


def open_position(conn, symbol: str, side: str, lot_multiplier: float = 1.0,
                  tag: str = "", fvg: dict = None) -> bool:
    info = mt5.symbol_info(symbol)
    tick = mt5.symbol_info_tick(symbol)
    if info is None or tick is None:
        return False

    df = fetch_rates(symbol, tf_name=CONFIG["exec_tf"])
    if df is None:
        return False
    df      = compute_indicators(df)
    atr     = df.iloc[-2]["atr"]
    filling = get_filling_mode(symbol)
    comment = f"{CONFIG['magic_number']}{('-'+tag) if tag else ''}"[:31]

    is_news = tag == "news"
    sl_mult = CONFIG["news_atr_sl_multiplier"] if is_news else CONFIG["atr_sl_multiplier"]

    # FVG limit order: entry at 50% equilibrium of the gap
    if fvg is not None:
        price   = fvg["mid"]
        # Validate FVG price is still within current market range (avoid retcode=10015)
        tick_now = mt5.symbol_info_tick(symbol)
        if tick_now is None:
            return False
        market_low  = min(tick_now.bid, tick_now.ask) * 0.995
        market_high = max(tick_now.bid, tick_now.ask) * 1.005
        if not (market_low <= price <= market_high):
            log.info(f"[order] {symbol} FVG mid {price:.5f} outside market range — skipping stale FVG")
            return False
        sl_dist = atr * sl_mult
        sl      = price - sl_dist if side == "BUY" else price + sl_dist
        tp1     = price + sl_dist * CONFIG["risk_reward_be"] if side == "BUY" \
                  else price - sl_dist * CONFIG["risk_reward_be"]
        tp      = price + sl_dist * CONFIG["risk_reward_ratio"] if side == "BUY" \
                  else price - sl_dist * CONFIG["risk_reward_ratio"]
        volume  = calc_lot_size(symbol, sl_dist, lot_multiplier)
        if volume <= 0:
            return False
        lim_type = mt5.ORDER_TYPE_BUY_LIMIT if side == "BUY" else mt5.ORDER_TYPE_SELL_LIMIT
        request  = {
            "action": mt5.TRADE_ACTION_PENDING, "symbol": symbol, "volume": volume,
            "type": lim_type, "price": price, "sl": sl, "tp": tp,
            "deviation": CONFIG["max_slippage_points"], "magic": CONFIG["magic_number"],
            "comment": comment, "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": filling,
        }
    else:
        price   = tick.ask if side == "BUY" else tick.bid
        sl_dist = atr * sl_mult
        sl      = price - sl_dist if side == "BUY" else price + sl_dist
        tp      = price + sl_dist * CONFIG["risk_reward_ratio"] if side == "BUY" \
                  else price - sl_dist * CONFIG["risk_reward_ratio"]
        volume  = calc_lot_size(symbol, sl_dist, lot_multiplier)
        if volume <= 0:
            return False
        request = {
            "action": mt5.TRADE_ACTION_DEAL, "symbol": symbol, "volume": volume,
            "type": mt5.ORDER_TYPE_BUY if side == "BUY" else mt5.ORDER_TYPE_SELL,
            "price": price, "sl": sl, "tp": tp,
            "deviation": CONFIG["max_slippage_points"], "magic": CONFIG["magic_number"],
            "comment": comment, "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": filling,
        }

    sl_pips = round(abs(price - sl) / pip_size(symbol))
    tp_pips = round(abs(tp - price) / pip_size(symbol))

    for attempt in range(1, CONFIG["max_order_retries"] + 1):
        result = mt5.order_send(request)
        if result is None:
            log.error(f"[order] None for {symbol}: {mt5.last_error()}")
            time.sleep(1.5)
            continue
        if result.retcode == mt5.TRADE_RETCODE_DONE:
            order_type_str = "Limit" if fvg else "Market"
            msg = (
                f"⚡ NEW SIGNAL EXECUTED ⚡\n"
                f"📈 Pair: {symbol} ({'Buy' if side=='BUY' else 'Sell'} {order_type_str})\n"
                f"🎯 Strategy: Confluence Engine{' ('+tag+')' if tag else ''}\n"
                f"📥 Entry Price: {price:.5f}\n"
                f"🛑 Stop Loss: {sl:.5f} ({sl_pips} Pips)\n"
                f"🎯 Take Profit 1: {price + abs(price-sl) if side=='BUY' else price - abs(price-sl):.5f} "
                f"(1:1 RR - Move to BE)\n"
                f"🎯 Take Profit 2: {tp:.5f} (1:{CONFIG['risk_reward_ratio']:.0f} RR - Target)\n"
                f"💰 Calculated Risk: {CONFIG['risk_per_trade_pct']:.2f}% (Exact Position Sized)"
            )
            log.info(f"[order] {msg}")
            telegram_send(msg)
            log_trade(conn, symbol, side, volume, price, sl, tp,
                      result.retcode, comment, result.order)
            return True
        if result.retcode == 10018:
            log_trade(conn, symbol, side, volume, price, sl, tp,
                      result.retcode, "market_closed", 0)
            return False
        if result.retcode in NON_RETRYABLE:
            log.warning(f"[order] {symbol} non-retryable {result.retcode}")
            log_trade(conn, symbol, side, volume, price, sl, tp,
                      result.retcode, result.comment, 0)
            return False
        log.warning(f"[order] {symbol} attempt {attempt} retcode={result.retcode}")
        time.sleep(1.5)
        tick = mt5.symbol_info_tick(symbol)
        if tick and fvg is None:
            request["price"] = tick.ask if side == "BUY" else tick.bid

    log_trade(conn, symbol, side, volume, price, sl, tp, -1, "exhausted_retries", 0)
    return False


def cancel_expired_fvg_orders():
    """Cancel pending FVG limit orders older than fvg_expiry_bars H1 candles."""
    orders = mt5.orders_get()
    if not orders:
        return
    now         = datetime.now(timezone.utc)
    expiry_secs = CONFIG["fvg_expiry_bars"] * 3600  # H1 bars in seconds
    for o in orders:
        if o.magic != CONFIG["magic_number"]:
            continue
        if o.type not in (mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT):
            continue
        age = now - datetime.fromtimestamp(o.time_setup, tz=timezone.utc)
        if age.total_seconds() >= expiry_secs:
            result = mt5.order_send({
                "action": mt5.TRADE_ACTION_REMOVE,
                "order":  o.ticket,
            })
            if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                log.info(f"[fvg] expired limit order {o.ticket} {o.symbol} cancelled")
                telegram_send(
                    f"🗑 FVG LIMIT EXPIRED: {o.symbol}\n"
                    f"  Order #{o.ticket} cancelled after {CONFIG['fvg_expiry_bars']} bars"
                )


def open_positions_count(symbol: str = None) -> int:
    positions = mt5.positions_get(symbol=symbol) if symbol else mt5.positions_get()
    if not positions:
        return 0
    return len([p for p in positions if p.magic == CONFIG["magic_number"]])


# =============================================================================
# POSITION MANAGEMENT — blueprint: BE at 1:1, trail at 1:2
# =============================================================================
def modify_sltp(position, new_sl, new_tp=None):
    result = mt5.order_send({
        "action":   mt5.TRADE_ACTION_SLTP,
        "position": position.ticket,
        "symbol":   position.symbol,
        "sl":       new_sl,
        "tp":       new_tp if new_tp is not None else position.tp,
    })
    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
        log.info(f"[manage] {position.symbol} SL -> {new_sl:.5f}")
    return result


def manage_open_positions(conn):
    positions = mt5.positions_get()
    if not positions:
        return
    for p in positions:
        if p.magic != CONFIG["magic_number"]:
            continue
        df = fetch_rates(p.symbol, tf_name=CONFIG["exec_tf"],
                         bars=CONFIG["atr_period"] + 10,
                         min_bars=CONFIG["atr_period"] + 5)
        if df is None:
            continue
        df  = compute_indicators(df)
        atr = df.iloc[-2]["atr"]
        pip = pip_size(p.symbol)
        is_buy = p.type == mt5.POSITION_TYPE_BUY
        t = mt5.symbol_info_tick(p.symbol)
        if t is None:
            continue
        cur   = t.bid if is_buy else t.ask
        prof  = (cur - p.price_open) if is_buy else (p.price_open - cur)
        sl_d  = abs(p.price_open - p.sl) if p.sl != 0 else atr * CONFIG["atr_sl_multiplier"]

        # Breakeven at 1:1 + commission buffer
        be_dist  = sl_d * CONFIG["risk_reward_be"]
        be_price = p.price_open + CONFIG["breakeven_buffer_pips"] * pip \
                   if is_buy else p.price_open - CONFIG["breakeven_buffer_pips"] * pip
        at_be    = (is_buy and p.sl >= p.price_open) or \
                   (not is_buy and p.sl != 0 and p.sl <= p.price_open)
        if prof >= be_dist and not at_be:
            modify_sltp(p, be_price)
            telegram_send(
                f"🔒 BREAKEVEN: {p.symbol} {'BUY' if is_buy else 'SELL'}\n"
                f"  SL moved to {be_price:.5f} (entry + commission buffer)"
            )

        # Trailing stop at 1:2
        if CONFIG["enable_trailing_stop"]:
            trail_dist = sl_d * CONFIG["risk_reward_trail"]
            step       = CONFIG["trailing_step_atr"] * atr
            if prof >= trail_dist and step > 0:
                new_sl = (cur - step) if is_buy else (cur + step)
                if is_buy and new_sl > p.sl:
                    modify_sltp(p, new_sl)
                elif not is_buy and (p.sl == 0 or new_sl < p.sl):
                    modify_sltp(p, new_sl)

        # Pyramiding
        if CONFIG["enable_pyramiding"]:
            applied = S.pyramid_applied.setdefault(p.symbol, set())
            for i, level in enumerate(CONFIG["pyramid_levels"]):
                if i in applied:
                    continue
                if prof >= level["atr_multiplier"] * atr:
                    side = "BUY" if is_buy else "SELL"
                    if open_position(conn, p.symbol, side,
                                     level["lot_multiplier"], tag=f"pyr{i}"):
                        applied.add(i)

# =============================================================================
# MARKET GUARDS
# =============================================================================
def is_weekend() -> bool:
    return datetime.now(timezone.utc).weekday() >= 5


def symbol_tradeable(symbol: str) -> bool:
    info = mt5.symbol_info(symbol)
    if info is None:
        return False
    if not info.visible:
        mt5.symbol_select(symbol, True)
        info = mt5.symbol_info(symbol)
    return info is not None and info.trade_mode != mt5.SYMBOL_TRADE_MODE_DISABLED


def in_cooldown(symbol: str) -> bool:
    until = S.cooldown_until.get(symbol)
    return until is not None and datetime.now(timezone.utc) < until


def refresh_cooldowns(conn):
    lookback = datetime.now(timezone.utc) - timedelta(
        minutes=CONFIG["cooldown_minutes_after_loss"] + 5)
    deals = mt5.history_deals_get(lookback, datetime.now(timezone.utc))
    if not deals:
        return
    for d in deals:
        if d.magic != CONFIG["magic_number"] or d.entry != mt5.DEAL_ENTRY_OUT:
            continue
        if d.profit < 0:
            end = datetime.fromtimestamp(d.time, tz=timezone.utc) + \
                  timedelta(minutes=CONFIG["cooldown_minutes_after_loss"])
            if end > datetime.now(timezone.utc):
                prev = S.cooldown_until.get(d.symbol)
                if prev is None or end > prev:
                    S.cooldown_until[d.symbol] = end


def currency_codes(symbol: str) -> list:
    m = re.match(r"^([A-Z]{3})([A-Z]{3})$", symbol)
    return [m.group(1), m.group(2)] if m else [symbol]


def currency_exposure_ok(symbol: str) -> bool:
    positions = mt5.positions_get()
    if not positions:
        return True
    codes = set(currency_codes(symbol))
    count = sum(1 for p in positions
                if p.magic == CONFIG["magic_number"]
                and codes & set(currency_codes(p.symbol)))
    return count < CONFIG["max_open_per_currency"]


# =============================================================================
# REPORTS & HEARTBEAT
# =============================================================================
def send_daily_summary(conn):
    info = mt5.account_info()
    if info is None:
        return
    row     = get_or_init_daily_row(conn)
    pnl     = info.equity - row["start_balance"]
    pnl_pct = pnl / row["start_balance"] * 100 if row["start_balance"] else 0
    today   = datetime.now(timezone.utc).date().isoformat()
    trades  = conn.execute(
        "SELECT COUNT(*) FROM trades WHERE ts LIKE ?", (f"{today}%",)).fetchone()[0]
    arrow   = "📈" if pnl >= 0 else "📉"
    telegram_send(
        f"📊 DAILY SUMMARY — {today}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{arrow} P&L : {'+'if pnl>=0 else ''}{pnl:.2f} ({pnl_pct:+.2f}%)\n"
        f"💰 Balance : {info.balance:.2f}\n"
        f"💎 Equity  : {info.equity:.2f}\n"
        f"📋 Trades  : {trades}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 Bot running — {len(CONFIG['symbols'])} pairs monitored"
    )
    log.info("[report] daily summary sent")


def send_weekly_report(conn):
    info = mt5.account_info()
    if info is None:
        return
    week_ago  = (datetime.now(timezone.utc) - timedelta(days=7)).date().isoformat()
    rows      = conn.execute(
        "SELECT realized_pnl FROM daily_pnl WHERE date >= ?", (week_ago,)).fetchall()
    total_pnl = sum(r[0] or 0 for r in rows)
    trades    = conn.execute(
        "SELECT COUNT(*) FROM trades WHERE ts >= ?", (week_ago,)).fetchone()[0]
    telegram_send(
        f"📅 WEEKLY REPORT\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Balance : {info.balance:.2f}\n"
        f"💎 Equity  : {info.equity:.2f}\n"
        f"📈 Week P&L: {'+'if total_pnl>=0 else ''}{total_pnl:.2f}\n"
        f"📋 Trades  : {trades}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Have a great weekend! 🎉"
    )
    log.info("[report] weekly report sent")


def check_scheduled_reports(conn):
    now   = datetime.now(timezone.utc)
    today = now.date()
    if S.last_daily_summary != today:
        S.drawdown_warned = False
    if now.hour == CONFIG["daily_summary_hour_utc"] and S.last_daily_summary != today:
        send_daily_summary(conn)
        S.last_daily_summary = today
    if (now.weekday() == CONFIG["weekly_report_day"] and
            now.hour == CONFIG["weekly_report_hour_utc"] and
            S.last_weekly_report != today):
        send_weekly_report(conn)
        S.last_weekly_report = today


def check_heartbeat():
    now      = datetime.now(timezone.utc)
    interval = timedelta(hours=CONFIG["telegram_heartbeat_hours"])
    if S.last_heartbeat is None or (now - S.last_heartbeat) >= interval:
        info     = mt5.account_info()
        sessions = ", ".join(get_current_sessions()) or "None"
        kz       = get_active_killzone() or "None"
        bal      = f"{info.balance:.2f}" if info else "N/A"
        eq       = f"{info.equity:.2f}"  if info else "N/A"
        telegram_send(
            f"💓 BOT HEARTBEAT — {now.strftime('%H:%M UTC')}\n"
            f"💰 Balance : {bal}  |  Equity: {eq}\n"
            f"🌍 Sessions: {sessions}\n"
            f"⚡ Killzone: {kz}\n"
            f"📊 Open trades: {open_positions_count()}\n"
            f"📰 Calendar: {len(fetch_forex_calendar())} events this week"
        )
        S.last_heartbeat = now


# =============================================================================
# SCAN CYCLE
# =============================================================================
def run_cycle(conn):
    account = mt5.account_info()
    if account is None:
        log.error("[bot] account_info() failed")
        return

    row       = get_or_init_daily_row(conn)
    today_pnl = account.equity - row["start_balance"]
    kz        = get_active_killzone() or "off-hours"
    log.info(f"[bot] bal={account.balance:.2f} eq={account.equity:.2f} "
             f"open={open_positions_count()} pnl={today_pnl:.2f} kz={kz}")

    refresh_cooldowns(conn)

    if trading_halted_for_today(conn):
        return
    if open_positions_count() >= CONFIG["max_open_trades"]:
        return

    # Upcoming event alerts + predictions
    send_upcoming_events_alert()
    for dt, title, country in get_upcoming_events(lookahead_minutes=20):
        key = f"{dt.isoformat()}_{title}"
        if key not in S.predicted_events:
            predict_news_event(title, country)
            S.predicted_events.add(key)

    # 30/30 blackout
    if news_blackout_active():
        log.info("[bot] 30-min pre-news blackout active — no new entries")
        return

    # Post-news breakout window
    news_result   = news_resume_window_active()
    is_news_cycle = news_result is not None
    if is_news_cycle:
        news_event_time, news_title, news_country = news_result
        log.info(f"[bot] post-news window: {news_title} ({news_country})")

    symbols_to_scan = CONFIG["news_symbols"] if is_news_cycle else CONFIG["symbols"]

    for symbol in symbols_to_scan:
        try:
            if not symbol_tradeable(symbol):
                continue
            sym_max_spread = max_spread_for(symbol)
            if spread_pips(symbol) > sym_max_spread:
                log.info(f"[bot] {symbol} spread {spread_pips(symbol):.1f} pips > "
                         f"{sym_max_spread} — skipping")
                continue
            if in_cooldown(symbol):
                continue
            if open_positions_count(symbol) >= CONFIG["max_positions_per_symbol"]:
                continue
            if not currency_exposure_ok(symbol):
                continue

            df = fetch_rates(symbol, tf_name=CONFIG["exec_tf"])
            if df is None:
                continue
            df   = compute_indicators(df)
            last = df.iloc[-2]

            # --- Post-news breakout ---
            if is_news_cycle:
                lookback  = CONFIG["news_breakout_lookback_bars"]
                pre_news  = df[df["time"] < news_event_time].tail(lookback)
                if len(pre_news) < lookback:
                    continue
                rng_high  = pre_news["high"].max()
                rng_low   = pre_news["low"].min()
                atr       = last["atr"]
                tick      = mt5.symbol_info_tick(symbol)
                if tick is None or atr <= 0:
                    continue
                threshold = CONFIG["news_breakout_atr_multiplier"] * atr
                if tick.ask > rng_high + threshold:
                    side = "BUY"
                elif tick.bid < rng_low - threshold:
                    side = "SELL"
                else:
                    continue
                entry = tick.ask if side == "BUY" else tick.bid
                sl_d  = atr * CONFIG["news_atr_sl_multiplier"]
                sl_p  = entry - sl_d if side == "BUY" else entry + sl_d
                tp_p  = entry + sl_d * CONFIG["risk_reward_ratio"] if side == "BUY" \
                        else entry - sl_d * CONFIG["risk_reward_ratio"]
                log_signal(conn, symbol, side, "news_breakout", entry, sl_p,
                           entry + sl_d if side == "BUY" else entry - sl_d,
                           tp_p, last["adx"], last["rsi"])
                log_scan(conn, symbol, side, "news_breakout", last["adx"], atr)
                telegram_send(
                    f"📰 NEWS BREAKOUT: {side} {symbol} [{news_title}]\n"
                    f"  📍 Entry: {entry:.5f}  🛑 SL: {sl_p:.5f}  🎯 TP: {tp_p:.5f}"
                )
                if not is_weekend():
                    open_position(conn, symbol, side,
                                  CONFIG["news_lot_multiplier"], tag="news")
                continue

            # --- Normal confluence engine ---
            side, fvg, reason = strategy_confluence_engine(symbol, df)
            log_scan(conn, symbol, side, reason, last["adx"], last["atr"])

            if side is None:
                continue

            tick  = mt5.symbol_info_tick(symbol)
            if tick is None:
                continue
            entry = fvg["mid"] if fvg else (tick.ask if side == "BUY" else tick.bid)
            atr   = last["atr"]
            sl_d  = atr * CONFIG["atr_sl_multiplier"]
            sl_p  = entry - sl_d if side == "BUY" else entry + sl_d
            tp1   = entry + sl_d if side == "BUY" else entry - sl_d
            tp2   = entry + sl_d * CONFIG["risk_reward_ratio"] if side == "BUY" \
                    else entry - sl_d * CONFIG["risk_reward_ratio"]
            sl_pips = round(sl_d / pip_size(symbol))

            log_signal(conn, symbol, side, "confluence_engine", entry, sl_p,
                       tp1, tp2, last["adx"], last["rsi"],
                       fvg["top"] if fvg else None,
                       fvg["bottom"] if fvg else None)

            sessions = ", ".join(get_current_sessions()) or "Off-hours"
            kz_name  = get_active_killzone() or "Off-hours"
            order_type = "Buy Limit" if (side == "BUY" and fvg) else \
                         "Sell Limit" if (side == "SELL" and fvg) else \
                         ("Buy" if side == "BUY" else "Sell")
            fvg_note = (f"\n🔲 FVG Box: {fvg['bottom']:.5f} — {fvg['top']:.5f}"
                        f"  |  Entry @ 50% EQ: {fvg['mid']:.5f}") if fvg else ""

            if CONFIG["telegram_notify_on_signal"]:
                weekend_note = " (weekend — queued)" if is_weekend() else ""
                telegram_send(
                    f"🚨 SESSION ALERT: {kz_name.upper()} 🚨\n"
                    f"⏰ Time: {datetime.now(timezone.utc).strftime('%H:%M UTC')}\n"
                    f"📊 Tracking: {len(CONFIG['symbols'])} Currency Pairs\n"
                    f"⚡ Status: Asian Range Mapped. Liquidity Swept. FVG Confirmed.\n"
                    f"--------------------------------------------------\n"
                    f"⚡ NEW SIGNAL EXECUTED ⚡{weekend_note}\n"
                    f"📈 Pair: {symbol} ({order_type})\n"
                    f"🎯 Strategy: Confluence Engine (Asia Sweep + FVG)\n"
                    f"📥 Entry Price: {entry:.5f}\n"
                    f"🛑 Stop Loss: {sl_p:.5f} ({sl_pips} Pips)\n"
                    f"🎯 Take Profit 1: {tp1:.5f} (1:1 RR - Move to BE)\n"
                    f"🎯 Take Profit 2: {tp2:.5f} (1:{CONFIG['risk_reward_ratio']:.0f} RR - Target)\n"
                    f"💰 Calculated Risk: {CONFIG['risk_per_trade_pct']:.2f}% (Exact Position Sized)\n"
                    f"🌍 Session: {sessions}{fvg_note}"
                )

            if not is_weekend():
                open_position(conn, symbol, side, fvg=fvg)

        except Exception:
            log.error(f"[bot] error on {symbol}:\n{traceback.format_exc()}")


# =============================================================================
# MT5 AUTO-RECONNECT
# =============================================================================
def ensure_mt5_connected() -> bool:
    if mt5.terminal_info() is not None:
        return True
    log.warning("[mt5] connection lost — reconnecting...")
    telegram_send("⚠️ MT5 connection lost — reconnecting...")
    time.sleep(5)
    return initialize_mt5()


def initialize_mt5() -> bool:
    m      = CONFIG["mt5"]
    kwargs = {"path": m["path"]}
    if m["login"]:
        kwargs.update(login=m["login"], password=m["password"], server=m["server"])
    ok = mt5.initialize(**kwargs)
    if not ok:
        log.error(f"[mt5] initialize failed: {mt5.last_error()}")
        return False
    log.info(f"[mt5] connected: {mt5.terminal_info()}")
    return True


# =============================================================================
# MAIN
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description="The Perfect Forex Bot")
    parser.add_argument("--predict", action="store_true",
                        help="Run CPI/news prediction and exit")
    args = parser.parse_args()

    if not initialize_mt5():
        sys.exit(1)

    # --predict mode: run prediction for all upcoming events and exit
    if args.predict:
        fetch_forex_calendar()
        events = get_upcoming_events(
            lookahead_minutes=CONFIG["startup_events_lookahead_minutes"])
        if not events:
            # No scheduled event — predict against next known high-impact event
            all_events = fetch_forex_calendar()
            if all_events:
                dt, title, country, _ = sorted(all_events)[0]
                predict_news_event(title, country)
            else:
                log.info("[predict] no high-impact events found this week")
        else:
            for dt, title, country in events:
                predict_news_event(title, country)
        mt5.shutdown()
        return

    conn     = db_connect()
    info     = mt5.account_info()
    sessions = ", ".join(get_current_sessions()) or "None active"
    kz       = get_active_killzone() or "None"

    # Send startup events alert
    send_upcoming_events_alert(
        lookahead=CONFIG["startup_events_lookahead_minutes"])

    telegram_send(
        f"🤖 THE PERFECT FOREX BOT — STARTED\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Balance  : {info.balance:.2f}\n"
        f"📊 Pairs    : {len(CONFIG['symbols'])}\n"
        f"🌍 Sessions : {sessions}\n"
        f"⚡ Killzone : {kz}\n"
        f"🎯 Strategy : ICT/SMC Confluence Engine\n"
        f"  Step 1 → 4H BOS/MSS Bias\n"
        f"  Step 2 → Asian Range Liquidity Sweep\n"
        f"  Step 3 → FVG Limit @ 50% Equilibrium\n"
        f"🛡 Risk     : {CONFIG['risk_per_trade_pct']}% per trade | "
        f"{CONFIG['max_daily_loss_pct']}% daily circuit breaker\n"
        f"📰 Calendar : Auto-fetch (30/30 rule active)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    )
    log.info(f"[bot] started — {len(CONFIG['symbols'])} pairs, ICT/SMC Confluence Engine")

    loops         = 0
    last_bar_time = {}
    exec_tf_const = TIMEFRAME_MAP[CONFIG["exec_tf"]]

    try:
        while True:
            if not ensure_mt5_connected():
                time.sleep(60)
                continue

            if is_weekend():
                log.info("[bot] weekend — sleeping 30 min")
                time.sleep(1800)
                continue

            check_session_change()
            check_heartbeat()
            check_scheduled_reports(conn)
            manage_open_positions(conn)

            new_bar = False
            for symbol in CONFIG["symbols"]:
                rates = mt5.copy_rates_from_pos(symbol, exec_tf_const, 0, 1)
                if rates is None or len(rates) == 0:
                    continue
                bar_time = rates[0]["time"]
                if last_bar_time.get(symbol) != bar_time:
                    last_bar_time[symbol] = bar_time
                    new_bar = True

            cancel_expired_fvg_orders()
            update_realized_pnl(conn)

            if new_bar:
                run_cycle(conn)
            else:
                loops += 1
                if loops % 30 == 0:
                    log.info("[bot] waiting for next candle close")

            time.sleep(CONFIG["poll_seconds"])

    except KeyboardInterrupt:
        log.info("[bot] stopped by user")
        _close_all_positions_to_breakeven(conn)
        telegram_send("🛑 Bot stopped by user — open positions moved to breakeven")
    except Exception:
        log.error(f"[bot] fatal:\n{traceback.format_exc()}")
        _close_all_positions_to_breakeven(conn)
        telegram_send("⚠️ Bot crashed — open positions moved to breakeven. Check logs.")
    finally:
        conn.close()
        mt5.shutdown()


if __name__ == "__main__":
    main()
