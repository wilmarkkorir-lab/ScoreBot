import MetaTrader5 as mt5
import pandas as pd
import numpy as np

mt5.initialize()

symbols = ['XAUUSD', 'EURUSD', 'GBPUSD', 'USDJPY']

def rsi(close, p=14):
    d = close.diff()
    g = d.clip(lower=0).ewm(alpha=1/p, adjust=False).mean()
    l = (-d.clip(upper=0)).ewm(alpha=1/p, adjust=False).mean()
    return 100 - (100 / (1 + g / l.replace(0, np.nan)))

def atr(df, p=14):
    pc = df['close'].shift(1)
    tr = pd.concat([df['high']-df['low'], (df['high']-pc).abs(), (df['low']-pc).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1/p, adjust=False).mean(), tr

def adx(df, tr, p=14):
    up = df['high'].diff(); dn = -df['low'].diff()
    pdm = np.where((up>dn)&(up>0), up, 0.0)
    mdm = np.where((dn>up)&(dn>0), dn, 0.0)
    a14 = tr.ewm(alpha=1/p, adjust=False).mean()
    pdi = 100*pd.Series(pdm,index=df.index).ewm(alpha=1/p,adjust=False).mean()/a14
    mdi = 100*pd.Series(mdm,index=df.index).ewm(alpha=1/p,adjust=False).mean()/a14
    dx  = (pdi-mdi).abs()/(pdi+mdi)*100
    return dx.ewm(alpha=1/p, adjust=False).mean(), pdi, mdi

def analyze(symbol, tf, tf_name):
    rates = mt5.copy_rates_from_pos(symbol, tf, 0, 300)
    df = pd.DataFrame(rates)
    df['close'] = df['close'].astype(float)
    df['high']  = df['high'].astype(float)
    df['low']   = df['low'].astype(float)
    df['open']  = df['open'].astype(float)

    ema50  = df['close'].ewm(span=50,  adjust=False).mean()
    ema200 = df['close'].ewm(span=200, adjust=False).mean()
    r      = rsi(df['close'])
    a, tr  = atr(df)
    adx_v, pdi, mdi = adx(df, tr)

    last = df.iloc[-2]
    hh = last['high'] > df['high'].iloc[-16:-2].max()
    ll = last['low']  < df['low'].iloc[-16:-2].min()
    hl = last['low']  > df['low'].iloc[-16:-2].min()
    lh = last['high'] < df['high'].iloc[-16:-2].max()

    if hh and hl:   smc = 'BOS BULLISH (HH+HL)'
    elif lh and ll: smc = 'BOS BEARISH (LH+LL)'
    elif hh:        smc = 'HH only - partial bull'
    elif ll:        smc = 'LL only - partial bear'
    else:           smc = 'RANGE / consolidation'

    ema_dir = 'BULL' if ema50.iloc[-2] > ema200.iloc[-2] else 'BEAR'
    rsi_v   = r.iloc[-2]
    adx_val = adx_v.iloc[-2]
    pdi_val = pdi.iloc[-2]
    mdi_val = mdi.iloc[-2]
    di_dir  = 'BULL' if pdi_val > mdi_val else 'BEAR'
    mom     = 'UP' if df['close'].iloc[-4:-1].mean() > df['close'].iloc[-13:-4].mean() else 'DOWN'

    # candles
    bull_c = sum(1 for i in range(-6,-1) if df.iloc[i]['close'] > df.iloc[i]['open'])
    candle_bias = 'BULL' if bull_c >= 3 else 'BEAR'

    # RSI divergence
    price_up = last['close'] > df['close'].iloc[-6]
    rsi_up   = r.iloc[-2] > r.iloc[-6]
    if not price_up and rsi_up:   div = 'BULL DIVERGENCE'
    elif price_up and not rsi_up: div = 'BEAR DIVERGENCE'
    else:                         div = 'none'

    votes_buy = votes_sell = 0
    if ema_dir == 'BULL': votes_buy += 2
    else: votes_sell += 2
    if di_dir == 'BULL': votes_buy += 2
    else: votes_sell += 2
    if mom == 'UP': votes_buy += 1
    else: votes_sell += 1
    if candle_bias == 'BULL': votes_buy += 1
    else: votes_sell += 1
    if 'BULL' in smc: votes_buy += 3
    elif 'BEAR' in smc: votes_sell += 3
    else:
        if ema_dir == 'BULL': votes_buy += 1
        else: votes_sell += 1
    if div == 'BULL DIVERGENCE': votes_buy += 2
    elif div == 'BEAR DIVERGENCE': votes_sell += 2
    if rsi_v >= 50: votes_buy += 1
    else: votes_sell += 1

    total = votes_buy + votes_sell
    side  = 'BUY' if votes_buy >= votes_sell else 'SELL'
    conf  = round(max(votes_buy, votes_sell) / total * 100)

    print(f'  [{tf_name}] EMA:{ema_dir} | DI:{di_dir}(+{pdi_val:.1f}/-{mdi_val:.1f}) | ADX:{adx_val:.1f} | RSI:{rsi_v:.1f} | MOM:{mom} | Candles:{candle_bias} | SMC:{smc} | Div:{div}')
    print(f'         => {side} {conf}%  (buy_votes={votes_buy} sell_votes={votes_sell})')
    return side, conf, a.iloc[-2]

print()
print('=' * 60)
print('DEEP MULTI-TIMEFRAME ANALYSIS - CPI DAY')
print('Current prices: XAUUSD=4151.65 | EURUSD=1.12104 | GBPUSD=1.32714 | USDJPY=158.297')
print('=' * 60)

USD_POS = {'USDJPY', 'USDCAD'}

for symbol in symbols:
    print(f'\n>>> {symbol}')
    results = []
    for tf, name in [(mt5.TIMEFRAME_H1,'H1'),(mt5.TIMEFRAME_H4,'H4'),(mt5.TIMEFRAME_D1,'D1')]:
        s, c, a = analyze(symbol, tf, name)
        results.append((s, c, a))

    # weighted final: D1=3, H4=2, H1=1
    buy_w = sell_w = 0
    for i, (s, c, _) in enumerate(results):
        w = [1, 2, 3][i]
        if s == 'BUY': buy_w += w * c
        else: sell_w += w * c

    final = 'BUY' if buy_w >= sell_w else 'SELL'
    total_w = buy_w + sell_w
    final_conf = round(max(buy_w, sell_w) / total_w * 100)

    tick = mt5.symbol_info_tick(symbol)
    atr_h1 = results[0][2]
    entry = tick.ask if final == 'BUY' else tick.bid
    sl = entry - atr_h1*2.5 if final == 'BUY' else entry + atr_h1*2.5
    tp = entry + atr_h1*5.0 if final == 'BUY' else entry - atr_h1*5.0

    print(f'  FINAL: {final} {final_conf}% confidence')
    print(f'  Entry: {entry:.5f}  SL: {sl:.5f}  TP: {tp:.5f}')

print()
print('=' * 60)
mt5.shutdown()
