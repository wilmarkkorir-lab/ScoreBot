"""
bot.py — EMA(50/200) + ADX trend-following bot for MetaTrader 5.
Single-file version. Edit the CONFIG block below, then run.

SECURITY NOTE: if you pasted a real Telegram bot token into this file or
into a chat, rotate it via @BotFather (/revoke) — treat any token that has
touched a chat log or a shared file as compromised.

Run:  python bot.py
Stop: Ctrl+C
"""

import logging
import os
import re
import sqlite3
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone

import MetaTrader5 as mt5
import numpy as np
import pandas as pd
import requests

# =============================================================================
# CONFIG — mirrors the JSON you gave me, as native Python values
# =============================================================================

CONFIG = {
    "symbols": [
        "XAUUSD", "EURUSD", "GBPUSD", "USDJPY", "AUDUSD",
        "USDCAD", "NZDUSD", "EURGBP", "EURJPY",
    ],
    "timeframe": "H1",
    "ema_fast": 50,
    "ema_slow": 200,
    "adx_period": 14,
    "adx_threshold": 20,
    "atr_period": 14,
    "atr_sl_multiplier": 1.5,
    "min_atr_value": 0.0001,
    "symbol_configs": {
        "XAUUSD": {"min_atr_value": 1.0},
        "EURUSD": {"min_atr_value": 0.0001},
        "GBPUSD": {"min_atr_value": 0.0001},
        "USDJPY": {"min_atr_value": 0.01},
        "AUDUSD": {"min_atr_value": 0.0001},
        "USDCAD": {"min_atr_value": 0.0001},
        "NZDUSD": {"min_atr_value": 0.0001},
        "EURGBP": {"min_atr_value": 0.00005},
        "EURJPY": {"min_atr_value": 0.01},
    },
    "risk_reward_ratio": 2.0,
    "risk_per_trade_pct": 1.0,
    "max_daily_loss_pct": 15.0,
    "max_open_trades": 4,
    "max_open_per_currency": 2,
    "max_positions_per_symbol": 1,  # guard against re-signaling the same trend every bar
    "magic_number": 123456,
    "poll_seconds": 60,
    "max_spread_points": 50,
    "cooldown_minutes_after_loss": 15,
    "trading_session": {
        "enabled": False,
        "start_hour_utc": 7,
        "end_hour_utc": 17,
    },
    "volume_lookback": 20,
    "volume_threshold": 0.0,
    "enable_trailing_stop": True,
    "trailing_activation_atr": 1.0,
    "trailing_step_atr": 0.5,
    "breakeven_pips": 10,
    "enable_limit_orders": False,
    "limit_offset_pips": 5,
    "max_order_retries": 3,
    "max_slippage_points": 20,
    "enable_pyramiding": False,
    "pyramid_levels": [
        {"atr_multiplier": 1.0, "lot_multiplier": 0.5},
        {"atr_multiplier": 2.0, "lot_multiplier": 0.3},
    ],
    "enable_strategy_voting": False,
    "strategies": ["ema_crossover", "ema_crossover"],
    "telegram": {
        "enabled": True,
        "bot_token": os.getenv("TELEGRAM_BOT_TOKEN", "8929699506:AAEbXIDodzrl905MFz3eRNiOTgcI-giwYf4"),
        "chat_id": os.getenv("TELEGRAM_CHAT_ID", "8792318186"),
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
# END CONFIG
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
TIMEFRAME = TIMEFRAME_MAP[CONFIG["timeframe"]]
NON_RETRYABLE = {10018, 10019, 10027, 10031, 10004}

# in-memory state (doesn't need to survive a restart)
_cooldown_until = {}          # symbol -> datetime
_pyramid_applied = {}         # symbol -> set of level indices already added


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------
def telegram_send(text: str) -> None:
    tg = CONFIG["telegram"]
    if not tg["enabled"] or not tg["bot_token"] or not tg["chat_id"]:
        return
    try:
        url = f"https://api.telegram.org/bot{tg['bot_token']}/sendMessage"
        requests.post(url, data={"chat_id": tg["chat_id"], "text": text}, timeout=10)
    except Exception as e:
        log.warning(f"[telegram] send failed: {e}")


# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------
def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(CONFIG["db_path"])
    conn.execute("""CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, symbol TEXT, side TEXT,
        adx REAL, ema_fast REAL, ema_slow REAL)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, symbol TEXT, side TEXT,
        volume REAL, price REAL, sl REAL, tp REAL, retcode INTEGER,
        comment TEXT, order_id INTEGER)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS daily_pnl (
        date TEXT PRIMARY KEY, start_balance REAL, realized_pnl REAL, halted INTEGER DEFAULT 0)""")
    conn.commit()
    return conn


def log_signal(conn, symbol, side, adx, ema_f, ema_s):
    conn.execute(
        "INSERT INTO signals (ts, symbol, side, adx, ema_fast, ema_slow) VALUES (?,?,?,?,?,?)",
        (datetime.now(timezone.utc).isoformat(), symbol, side, adx, ema_f, ema_s))
    conn.commit()


def log_trade(conn, symbol, side, volume, price, sl, tp, retcode, comment, order_id):
    conn.execute(
        """INSERT INTO trades (ts, symbol, side, volume, price, sl, tp, retcode, comment, order_id)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (datetime.now(timezone.utc).isoformat(), symbol, side, volume, price,
         sl, tp, retcode, comment, order_id))
    conn.commit()


# ---------------------------------------------------------------------------
# Daily loss circuit breaker
# ---------------------------------------------------------------------------
def get_or_init_daily_row(conn) -> sqlite3.Row:
    today = datetime.now(timezone.utc).date().isoformat()
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM daily_pnl WHERE date=?", (today,)).fetchone()
    if row is None:
        info = mt5.account_info()
        start_balance = info.balance if info else 0.0
        conn.execute(
            "INSERT INTO daily_pnl (date, start_balance, realized_pnl, halted) VALUES (?,?,?,0)",
            (today, start_balance, 0.0))
        conn.commit()
        row = conn.execute("SELECT * FROM daily_pnl WHERE date=?", (today,)).fetchone()
    return row


def trading_halted_for_today(conn) -> bool:
    row = get_or_init_daily_row(conn)
    info = mt5.account_info()
    if info is None or row["start_balance"] == 0:
        return False
    day_pnl_pct = (info.equity - row["start_balance"]) / row["start_balance"] * 100.0

    if day_pnl_pct <= -CONFIG["max_daily_loss_pct"]:
        if not row["halted"]:
            msg = (f"🛑 DAILY LOSS LIMIT HIT: {day_pnl_pct:.2f}% "
                   f"(limit -{CONFIG['max_daily_loss_pct']}%). Pausing new entries until tomorrow.")
            log.warning(msg)
            telegram_send(msg)
            today = datetime.now(timezone.utc).date().isoformat()
            conn.execute("UPDATE daily_pnl SET halted=1 WHERE date=?", (today,))
            conn.commit()
        return True
    return False


# ---------------------------------------------------------------------------
# Session / market guards
# ---------------------------------------------------------------------------
def is_weekend() -> bool:
    return datetime.now(timezone.utc).weekday() >= 5


def in_trading_session() -> bool:
    sess = CONFIG["trading_session"]
    if not sess["enabled"]:
        return True
    hour = datetime.now(timezone.utc).hour
    return sess["start_hour_utc"] <= hour < sess["end_hour_utc"]


def symbol_tradeable(symbol: str) -> bool:
    info = mt5.symbol_info(symbol)
    if info is None:
        return False
    if not info.visible:
        mt5.symbol_select(symbol, True)
        info = mt5.symbol_info(symbol)
    return info is not None and info.trade_mode != mt5.SYMBOL_TRADE_MODE_DISABLED


def spread_ok(symbol: str) -> bool:
    info = mt5.symbol_info(symbol)
    if info is None:
        return False
    return info.spread <= CONFIG["max_spread_points"]


def in_cooldown(symbol: str) -> bool:
    until = _cooldown_until.get(symbol)
    return until is not None and datetime.now(timezone.utc) < until


def refresh_cooldowns(conn):
    """Check recently closed deals for this magic number; start a cooldown on losers."""
    lookback = datetime.now(timezone.utc) - timedelta(minutes=CONFIG["cooldown_minutes_after_loss"] + 5)
    deals = mt5.history_deals_get(lookback, datetime.now(timezone.utc))
    if not deals:
        return
    for d in deals:
        if d.magic != CONFIG["magic_number"] or d.entry != mt5.DEAL_ENTRY_OUT:
            continue
        if d.profit < 0:
            cooldown_end = datetime.fromtimestamp(d.time, tz=timezone.utc) + \
                timedelta(minutes=CONFIG["cooldown_minutes_after_loss"])
            if cooldown_end > datetime.now(timezone.utc):
                prev = _cooldown_until.get(d.symbol)
                if prev is None or cooldown_end > prev:
                    _cooldown_until[d.symbol] = cooldown_end


def currency_codes(symbol: str) -> list:
    """XAUUSD -> ['XAU','USD']; EURJPY -> ['EUR','JPY']; falls back to whole symbol."""
    m = re.match(r"^([A-Z]{3})([A-Z]{3})$", symbol)
    return [m.group(1), m.group(2)] if m else [symbol]


def currency_exposure_ok(symbol: str) -> bool:
    positions = mt5.positions_get()
    if not positions:
        return True
    codes = set(currency_codes(symbol))
    count = 0
    for p in positions:
        if p.magic != CONFIG["magic_number"]:
            continue
        if codes & set(currency_codes(p.symbol)):
            count += 1
    return count < CONFIG["max_open_per_currency"]


# ---------------------------------------------------------------------------
# Indicators
# ---------------------------------------------------------------------------
def fetch_rates(symbol: str, bars: int = 400):
    rates = mt5.copy_rates_from_pos(symbol, TIMEFRAME, 0, bars)
    if rates is None or len(rates) < CONFIG["ema_slow"] + CONFIG["adx_period"] + 5:
        return None
    df = pd.DataFrame(rates)
    df["time"] = pd.to_datetime(df["time"], unit="s")
    return df


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ema_fast"] = df["close"].ewm(span=CONFIG["ema_fast"], adjust=False).mean()
    df["ema_slow"] = df["close"].ewm(span=CONFIG["ema_slow"], adjust=False).mean()

    high, low, close = df["high"], df["low"], df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    df["atr"] = tr.ewm(alpha=1 / CONFIG["atr_period"], adjust=False).mean()

    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    atr_for_di = tr.ewm(alpha=1 / CONFIG["adx_period"], adjust=False).mean()
    plus_di = 100 * pd.Series(plus_dm, index=df.index).ewm(alpha=1 / CONFIG["adx_period"], adjust=False).mean() / atr_for_di
    minus_di = 100 * pd.Series(minus_dm, index=df.index).ewm(alpha=1 / CONFIG["adx_period"], adjust=False).mean() / atr_for_di
    dx = (plus_di - minus_di).abs() / (plus_di + minus_di) * 100
    df["adx"] = dx.ewm(alpha=1 / CONFIG["adx_period"], adjust=False).mean()

    df["vol_avg"] = df["tick_volume"].rolling(CONFIG["volume_lookback"]).mean()
    return df


def min_atr_for(symbol: str) -> float:
    override = CONFIG["symbol_configs"].get(symbol, {})
    return override.get("min_atr_value", CONFIG["min_atr_value"])


# ---------------------------------------------------------------------------
# Strategies (voting scaffold — only ema_crossover implemented today)
# ---------------------------------------------------------------------------
def strategy_ema_crossover(df: pd.DataFrame):
    last, prev = df.iloc[-2], df.iloc[-3]
    trend_up = last["ema_fast"] > last["ema_slow"]
    trend_down = last["ema_fast"] < last["ema_slow"]
    adx_ok = last["adx"] >= CONFIG["adx_threshold"]
    adx_rising = last["adx"] > prev["adx"]
    if trend_up and adx_ok and adx_rising:
        return "BUY"
    if trend_down and adx_ok and adx_rising:
        return "SELL"
    return None


STRATEGY_FUNCS = {"ema_crossover": strategy_ema_crossover}


def get_signal(symbol: str, df: pd.DataFrame):
    last, prev = df.iloc[-2], df.iloc[-3]

    if last["atr"] < min_atr_for(symbol):
        return None  # market too quiet, don't trade noise

    if CONFIG["volume_threshold"] > 0 or not np.isnan(last["vol_avg"]):
        avg_vol = last["vol_avg"]
        if pd.notna(avg_vol) and avg_vol > 0:
            if last["tick_volume"] < avg_vol * (1 + CONFIG["volume_threshold"]):
                return None  # no volume confirmation

    if not CONFIG["enable_strategy_voting"]:
        return strategy_ema_crossover(df)

    votes = [STRATEGY_FUNCS[name](df) for name in CONFIG["strategies"] if name in STRATEGY_FUNCS]
    votes = [v for v in votes if v is not None]
    if not votes:
        return None
    buy_votes = votes.count("BUY")
    sell_votes = votes.count("SELL")
    if buy_votes > len(CONFIG["strategies"]) / 2:
        return "BUY"
    if sell_votes > len(CONFIG["strategies"]) / 2:
        return "SELL"
    return None


# ---------------------------------------------------------------------------
# Position sizing
# ---------------------------------------------------------------------------
def calc_lot_size(symbol: str, sl_distance_price: float, lot_multiplier: float = 1.0) -> float:
    info = mt5.symbol_info(symbol)
    account = mt5.account_info()
    if info is None or account is None or sl_distance_price <= 0:
        return 0.0

    risk_amount = account.balance * (CONFIG["risk_per_trade_pct"] / 100.0) * lot_multiplier
    tick_size = info.trade_tick_size or info.point
    tick_value = info.trade_tick_value
    if tick_size <= 0 or tick_value <= 0:
        return 0.0

    ticks_at_risk = sl_distance_price / tick_size
    loss_per_lot = ticks_at_risk * tick_value
    if loss_per_lot <= 0:
        return 0.0

    lots = risk_amount / loss_per_lot
    lots = max(CONFIG["min_lot_size"], min(CONFIG["max_lot_size"], lots))
    step = info.volume_step or 0.01
    lots = round(round(lots / step) * step, 2)
    return max(lots, info.volume_min or CONFIG["min_lot_size"])


# ---------------------------------------------------------------------------
# Order execution
# ---------------------------------------------------------------------------
def get_filling_mode(symbol: str) -> int:
    info = mt5.symbol_info(symbol)
    if info is None:
        return mt5.ORDER_FILLING_IOC
    flags = info.filling_mode
    if flags & mt5.SYMBOL_FILLING_FOK:
        return mt5.ORDER_FILLING_FOK
    if flags & mt5.SYMBOL_FILLING_IOC:
        return mt5.ORDER_FILLING_IOC
    return mt5.ORDER_FILLING_RETURN


def pip_size(symbol: str) -> float:
    info = mt5.symbol_info(symbol)
    if info is None:
        return 0.0001
    # 3/5-digit brokers use 10x point as a "pip"
    return info.point * (10 if info.digits in (3, 5) else 1)


def open_position(conn, symbol: str, side: str, lot_multiplier: float = 1.0, tag: str = "") -> bool:
    info = mt5.symbol_info(symbol)
    tick = mt5.symbol_info_tick(symbol)
    if info is None or tick is None:
        log.error(f"[order] no symbol/tick info for {symbol}")
        return False

    df = compute_indicators(fetch_rates(symbol))
    atr = df.iloc[-2]["atr"]
    price = tick.ask if side == "BUY" else tick.bid
    sl_dist = atr * CONFIG["atr_sl_multiplier"]
    tp_dist = sl_dist * CONFIG["risk_reward_ratio"]

    sl = price - sl_dist if side == "BUY" else price + sl_dist
    tp = price + tp_dist if side == "BUY" else price - tp_dist

    volume = calc_lot_size(symbol, sl_dist, lot_multiplier)
    if volume <= 0:
        log.warning(f"[order] computed lot size 0 for {symbol}, skipping")
        return False

    order_type = mt5.ORDER_TYPE_BUY if side == "BUY" else mt5.ORDER_TYPE_SELL
    filling = get_filling_mode(symbol)
    comment = f"{CONFIG['magic_number']}{('-' + tag) if tag else ''}"[:31]

    if CONFIG["enable_limit_orders"]:
        offset = CONFIG["limit_offset_pips"] * pip_size(symbol)
        limit_price = price - offset if side == "BUY" else price + offset
        order_type_pending = mt5.ORDER_TYPE_BUY_LIMIT if side == "BUY" else mt5.ORDER_TYPE_SELL_LIMIT
        request = {
            "action": mt5.TRADE_ACTION_PENDING,
            "symbol": symbol, "volume": volume, "type": order_type_pending,
            "price": limit_price, "sl": sl, "tp": tp,
            "deviation": CONFIG["max_slippage_points"], "magic": CONFIG["magic_number"],
            "comment": comment, "type_time": mt5.ORDER_TIME_GTC, "type_filling": filling,
        }
    else:
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol, "volume": volume, "type": order_type,
            "price": price, "sl": sl, "tp": tp,
            "deviation": CONFIG["max_slippage_points"], "magic": CONFIG["magic_number"],
            "comment": comment, "type_time": mt5.ORDER_TIME_GTC, "type_filling": filling,
        }

    for attempt in range(1, CONFIG["max_order_retries"] + 1):
        result = mt5.order_send(request)

        if result is None:
            log.error(f"[order] order_send returned None for {symbol}, mt5 error={mt5.last_error()}")
            time.sleep(1.5)
            continue

        if result.retcode == mt5.TRADE_RETCODE_DONE:
            msg = f"✅ {side} {symbol} {volume} lots @ {price:.5f} | SL {sl:.5f} TP {tp:.5f} {tag}"
            log.info(f"[order] {msg}")
            telegram_send(msg)
            log_trade(conn, symbol, side, volume, price, sl, tp, result.retcode, comment, result.order)
            return True

        if result.retcode == 10018:
            log.info(f"[order] {symbol} market closed, skipping")
            log_trade(conn, symbol, side, volume, price, sl, tp, result.retcode, "market_closed", 0)
            return False

        if result.retcode in NON_RETRYABLE:
            log.warning(f"[order] {symbol} non-retryable retcode {result.retcode}: {result.comment}")
            log_trade(conn, symbol, side, volume, price, sl, tp, result.retcode, result.comment, 0)
            return False

        log.warning(f"[order] {symbol} attempt {attempt}/{CONFIG['max_order_retries']} "
                    f"failed retcode={result.retcode} comment={result.comment}")
        time.sleep(1.5)
        tick = mt5.symbol_info_tick(symbol)
        if tick and not CONFIG["enable_limit_orders"]:
            request["price"] = tick.ask if side == "BUY" else tick.bid

    log.error(f"[order] {symbol} {side} failed after {CONFIG['max_order_retries']} attempts")
    log_trade(conn, symbol, side, volume, price, sl, tp, -1, "exhausted_retries", 0)
    return False


def open_positions_count(symbol: str = None) -> int:
    positions = mt5.positions_get(symbol=symbol) if symbol else mt5.positions_get()
    if not positions:
        return 0
    return len([p for p in positions if p.magic == CONFIG["magic_number"]])


# ---------------------------------------------------------------------------
# Position management: breakeven + trailing stop + pyramiding
# ---------------------------------------------------------------------------
def modify_sltp(position, new_sl, new_tp=None):
    request = {
        "action": mt5.TRADE_ACTION_SLTP,
        "position": position.ticket,
        "symbol": position.symbol,
        "sl": new_sl,
        "tp": new_tp if new_tp is not None else position.tp,
    }
    result = mt5.order_send(request)
    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
        log.info(f"[manage] {position.symbol} SL moved to {new_sl:.5f}")
    return result


def manage_open_positions(conn):
    positions = mt5.positions_get()
    if not positions:
        return

    for p in positions:
        if p.magic != CONFIG["magic_number"]:
            continue

        df = fetch_rates(p.symbol, bars=CONFIG["atr_period"] + 10)
        if df is None:
            continue
        df = compute_indicators(df)
        atr = df.iloc[-2]["atr"]
        pip = pip_size(p.symbol)
        is_buy = p.type == mt5.POSITION_TYPE_BUY
        current_price = mt5.symbol_info_tick(p.symbol).bid if is_buy else mt5.symbol_info_tick(p.symbol).ask
        profit_price = (current_price - p.price_open) if is_buy else (p.price_open - current_price)

        # --- breakeven ---
        breakeven_dist = CONFIG["breakeven_pips"] * pip
        already_at_be = (is_buy and p.sl >= p.price_open) or (not is_buy and p.sl <= p.price_open)
        if profit_price >= breakeven_dist and not already_at_be:
            modify_sltp(p, p.price_open)

        # --- trailing stop ---
        if CONFIG["enable_trailing_stop"]:
            activation_dist = CONFIG["trailing_activation_atr"] * atr
            step_dist = CONFIG["trailing_step_atr"] * atr
            if profit_price >= activation_dist and step_dist > 0:
                if is_buy:
                    new_sl = current_price - step_dist
                    if new_sl > p.sl:
                        modify_sltp(p, new_sl)
                else:
                    new_sl = current_price + step_dist
                    if p.sl == 0 or new_sl < p.sl:
                        modify_sltp(p, new_sl)

        # --- pyramiding ---
        if CONFIG["enable_pyramiding"]:
            applied = _pyramid_applied.setdefault(p.symbol, set())
            for i, level in enumerate(CONFIG["pyramid_levels"]):
                if i in applied:
                    continue
                if profit_price >= level["atr_multiplier"] * atr:
                    side = "BUY" if is_buy else "SELL"
                    if open_position(conn, p.symbol, side, level["lot_multiplier"], tag=f"pyr{i}"):
                        applied.add(i)


# ---------------------------------------------------------------------------
# One scan cycle
# ---------------------------------------------------------------------------
def run_cycle(conn):
    account = mt5.account_info()
    if account is None:
        log.error("[bot] account_info() failed, MT5 connection may be down")
        return

    row = get_or_init_daily_row(conn)
    today_pnl = account.equity - row["start_balance"]
    log.info(f"[bot] cycle - balance={account.balance:.2f} equity={account.equity:.2f} "
              f"open={open_positions_count()} today_pnl={today_pnl:.2f}")

    refresh_cooldowns(conn)

    if trading_halted_for_today(conn):
        log.info("[bot] daily loss limit active, skipping new entries")
        return
    if not in_trading_session():
        log.info("[bot] outside configured trading session, skipping new entries")
        return
    if open_positions_count() >= CONFIG["max_open_trades"]:
        log.info("[bot] max concurrent trades reached, skipping new entries")
        return

    for symbol in CONFIG["symbols"]:
        try:
            if not symbol_tradeable(symbol):
                continue
            if not spread_ok(symbol):
                log.info(f"[bot] {symbol} spread too wide, skipping")
                continue
            if in_cooldown(symbol):
                log.info(f"[bot] {symbol} in post-loss cooldown, skipping")
                continue
            if open_positions_count(symbol) >= CONFIG["max_positions_per_symbol"]:
                continue  # already in this symbol - EMA/ADX signal will keep firing every bar, don't stack
            if not currency_exposure_ok(symbol):
                log.info(f"[bot] {symbol} currency exposure limit reached, skipping")
                continue

            df = fetch_rates(symbol)
            if df is None:
                log.warning(f"[bot] not enough bars for {symbol}, skipping")
                continue
            df = compute_indicators(df)

            side = get_signal(symbol, df)
            if side is None:
                continue

            last = df.iloc[-2]
            log.info(f"[signal] {side} {symbol} - EMA{CONFIG['ema_fast']}/{CONFIG['ema_slow']} "
                      f"ADX={last['adx']:.1f} ATR={last['atr']:.5f}")
            log_signal(conn, symbol, side, last["adx"], last["ema_fast"], last["ema_slow"])

            if is_weekend():
                log.info(f"[bot] weekend - not sending order for {symbol}")
                continue

            open_position(conn, symbol, side)

        except Exception:
            log.error(f"[bot] error processing {symbol}:\n{traceback.format_exc()}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def initialize_mt5() -> bool:
    m = CONFIG["mt5"]
    kwargs = {"path": m["path"]}
    if m["login"]:
        kwargs.update(login=m["login"], password=m["password"], server=m["server"])
    ok = mt5.initialize(**kwargs)
    if not ok:
        log.error(f"[mt5] initialize failed: {mt5.last_error()}")
        return False
    log.info(f"[mt5] connected: {mt5.terminal_info()}")
    return True


def main():
    if not initialize_mt5():
        sys.exit(1)

    conn = db_connect()
    telegram_send("🤖 Bot started")
    log.info(f"[bot] started on {CONFIG['timeframe']} watching {len(CONFIG['symbols'])} pairs")

    loops = 0
    last_bar_time = {}

    try:
        while True:
            if is_weekend():
                log.info("[bot] weekend - market closed, sleeping 30 min")
                time.sleep(1800)
                continue

            manage_open_positions(conn)  # runs every poll, not just on new bar

            new_bar = False
            for symbol in CONFIG["symbols"]:
                rates = mt5.copy_rates_from_pos(symbol, TIMEFRAME, 0, 1)
                if rates is None or len(rates) == 0:
                    continue
                bar_time = rates[0]["time"]
                if last_bar_time.get(symbol) != bar_time:
                    last_bar_time[symbol] = bar_time
                    new_bar = True

            if new_bar:
                run_cycle(conn)
            else:
                loops += 1
                if loops % 30 == 0:
                    log.info("[bot] heartbeat - waiting for next H1 candle close")

            time.sleep(CONFIG["poll_seconds"])

    except KeyboardInterrupt:
        log.info("[bot] stopped by user")
        telegram_send("🛑 Bot stopped (manual)")
    except Exception:
        log.error(f"[bot] fatal error:\n{traceback.format_exc()}")
        telegram_send("⚠️ Bot crashed - check logs")
    finally:
        conn.close()
        mt5.shutdown()


if __name__ == "__main__":
    main()
