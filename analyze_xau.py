import MetaTrader5 as mt5
import pandas as pd
import numpy as np

mt5.initialize()
symbol = 'XAUUSD'

def get_df(tf, bars=500):
    rates = mt5.copy_rates_from_pos(symbol, tf, 0, bars)
    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    for c in ['open','high','low','close']:
        df[c] = df[c].astype(float)
    return df

def calc_all(df):
    df['ema20']  = df['close'].ewm(span=20,  adjust=False).mean()
    df['ema50']  = df['close'].ewm(span=50,  adjust=False).mean()
    df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()
    d = df['close'].diff()
    g = d.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
    l = (-d.clip(upper=0)).ewm(alpha=1/14, adjust=False).mean()
    df['rsi'] = 100 - (100/(1+g/l.replace(0,np.nan)))
    pc = df['close'].shift(1)
    tr = pd.concat([df['high']-df['low'],(df['high']-pc).abs(),(df['low']-pc).abs()],axis=1).max(axis=1)
    df['atr'] = tr.ewm(alpha=1/14, adjust=False).mean()
    up = df['high'].diff(); dn = -df['low'].diff()
    pdm = np.where((up>dn)&(up>0), up, 0.0)
    mdm = np.where((dn>up)&(dn>0), dn, 0.0)
    a14 = tr.ewm(alpha=1/14, adjust=False).mean()
    df['pdi'] = 100*pd.Series(pdm,index=df.index).ewm(alpha=1/14,adjust=False).mean()/a14
    df['mdi'] = 100*pd.Series(mdm,index=df.index).ewm(alpha=1/14,adjust=False).mean()/a14
    dx = (df['pdi']-df['mdi']).abs()/(df['pdi']+df['mdi'])*100
    df['adx'] = dx.ewm(alpha=1/14, adjust=False).mean()
    df['macd']   = df['close'].ewm(span=12,adjust=False).mean() - df['close'].ewm(span=26,adjust=False).mean()
    df['signal_line'] = df['macd'].ewm(span=9,adjust=False).mean()
    df['hist']   = df['macd'] - df['signal_line']
    df['bb_mid'] = df['close'].rolling(20).mean()
    std = df['close'].rolling(20).std()
    df['bb_up']  = df['bb_mid'] + 2*std
    df['bb_dn']  = df['bb_mid'] - 2*std
    low14  = df['low'].rolling(14).min()
    high14 = df['high'].rolling(14).max()
    df['stoch_k'] = 100*(df['close']-low14)/(high14-low14)
    df['stoch_d'] = df['stoch_k'].rolling(3).mean()
    return df

print()
print('=' * 65)
print('XAUUSD DEEP ANALYSIS - CPI DAY (15:30 EAT / 12:30 UTC)')
print('=' * 65)

buy_votes_total  = 0
sell_votes_total = 0
tf_weight = {'M15': 1, 'H1': 3, 'H4': 5, 'D1': 7}

for tf, name, label in [
    (mt5.TIMEFRAME_M15, 'M15', '15-Min  (entry timing)'),
    (mt5.TIMEFRAME_H1,  'H1',  'Hourly  (main signal) '),
    (mt5.TIMEFRAME_H4,  'H4',  '4-Hour  (trend)       '),
    (mt5.TIMEFRAME_D1,  'D1',  'Daily   (big picture) '),
]:
    df = get_df(tf)
    df = calc_all(df)
    L  = df.iloc[-2]
    L2 = df.iloc[-3]

    hh = L['high'] > df['high'].iloc[-16:-2].max()
    ll = L['low']  < df['low'].iloc[-16:-2].min()
    hl = L['low']  > df['low'].iloc[-16:-2].min()
    lh = L['high'] < df['high'].iloc[-16:-2].max()
    if hh and hl:   smc='BOS BULLISH (HH+HL)'
    elif lh and ll: smc='BOS BEARISH (LH+LL)'
    elif hh:        smc='Higher High (partial bull)'
    elif ll:        smc='Lower Low   (partial bear)'
    else:           smc='Consolidation / Range'

    price_up = L['close'] > df['close'].iloc[-7]
    rsi_up   = L['rsi']   > df['rsi'].iloc[-7]
    if not price_up and rsi_up:   div='BULLISH DIVERGENCE (+2 buy)'
    elif price_up and not rsi_up: div='BEARISH DIVERGENCE (+2 sell)'
    else:                         div='None'

    if L['hist'] > 0 and L2['hist'] < 0:   macd_sig='BULLISH CROSS'
    elif L['hist'] < 0 and L2['hist'] > 0: macd_sig='BEARISH CROSS'
    elif L['hist'] > L2['hist']:            macd_sig='Histogram rising (bullish momentum)'
    else:                                   macd_sig='Histogram falling (bearish momentum)'

    bb_pct = (L['close'] - L['bb_dn']) / (L['bb_up'] - L['bb_dn']) * 100
    if bb_pct < 15:   bb_pos=f'NEAR LOWER BAND - oversold ({bb_pct:.0f}%)'
    elif bb_pct > 85: bb_pos=f'NEAR UPPER BAND - overbought ({bb_pct:.0f}%)'
    else:             bb_pos=f'Mid band ({bb_pct:.0f}%)'

    stoch_pos = 'OVERSOLD' if L['stoch_k'] < 20 else ('OVERBOUGHT' if L['stoch_k'] > 80 else f'{L["stoch_k"]:.1f}')
    stoch_dir = 'K above D (bullish)' if L['stoch_k'] > L['stoch_d'] else 'K below D (bearish)'

    ema_trend = 'BULLISH' if L['ema20']>L['ema50']>L['ema200'] else \
                'BEARISH' if L['ema20']<L['ema50']<L['ema200'] else 'MIXED'

    # score this timeframe
    bv = sv = 0
    w  = tf_weight[name]

    if L['ema50'] > L['ema200']:  bv += 2*w
    else:                          sv += 2*w
    if L['pdi'] > L['mdi']:       bv += 2*w
    else:                          sv += 2*w
    if 'BULL' in smc:              bv += 3*w
    elif 'BEAR' in smc:            sv += 3*w
    else:
        if L['ema50'] > L['ema200']: bv += w
        else:                         sv += w
    if 'BULLISH DIV' in div:       bv += 2*w
    elif 'BEARISH DIV' in div:     sv += 2*w
    if L['hist'] > L2['hist']:     bv += w
    else:                           sv += w
    if bb_pct < 20:                bv += w
    elif bb_pct > 80:              sv += w
    if L['stoch_k'] < 25:          bv += w
    elif L['stoch_k'] > 75:        sv += w
    if L['rsi'] < 35:              bv += 2*w
    elif L['rsi'] > 65:            sv += 2*w
    elif L['rsi'] >= 50:           bv += w
    else:                           sv += w

    buy_votes_total  += bv
    sell_votes_total += sv
    tf_total = bv + sv
    tf_side  = 'BUY' if bv >= sv else 'SELL'
    tf_conf  = round(max(bv,sv)/tf_total*100)

    print(f'\n--- {name} ({label}) ---')
    print(f'  Price    : {L["close"]:.2f}  |  H:{L["high"]:.2f}  L:{L["low"]:.2f}')
    print(f'  EMA Stack: 20={L["ema20"]:.2f}  50={L["ema50"]:.2f}  200={L["ema200"]:.2f}  -> {ema_trend}')
    print(f'  RSI      : {L["rsi"]:.1f}  {"<< OVERSOLD" if L["rsi"]<30 else (">> OVERBOUGHT" if L["rsi"]>70 else "")}')
    print(f'  RSI Div  : {div}')
    print(f'  ADX/DI   : ADX={L["adx"]:.1f}  +DI={L["pdi"]:.1f}  -DI={L["mdi"]:.1f}  -> {"BULL pressure" if L["pdi"]>L["mdi"] else "BEAR pressure"}')
    print(f'  MACD     : {macd_sig}')
    print(f'  Bollinger: {bb_pos}')
    print(f'  Stoch    : {stoch_pos}  |  {stoch_dir}')
    print(f'  SMC      : {smc}')
    print(f'  ATR      : {L["atr"]:.2f}')
    print(f'  >> {name} VERDICT: {tf_side} {tf_conf}%  (buy={bv} sell={sv})')

# final weighted verdict
total = buy_votes_total + sell_votes_total
final_side = 'BUY' if buy_votes_total >= sell_votes_total else 'SELL'
final_conf = round(max(buy_votes_total, sell_votes_total) / total * 100)

tick = mt5.symbol_info_tick(symbol)
h1_df = get_df(mt5.TIMEFRAME_H1)
h1_df = calc_all(h1_df)
atr_h1 = h1_df.iloc[-2]['atr']

entry = tick.ask if final_side == 'BUY' else tick.bid
sl    = entry - atr_h1*2.5 if final_side == 'BUY' else entry + atr_h1*2.5
tp    = entry + atr_h1*5.0 if final_side == 'BUY' else entry - atr_h1*5.0

print()
print('=' * 65)
print(f'LIVE PRICE : Bid={tick.bid:.2f}  Ask={tick.ask:.2f}')
print(f'FINAL CALL : {final_side}  {final_conf}% confidence')
print(f'             (weighted buy={buy_votes_total}  sell={sell_votes_total})')
print(f'Entry      : {entry:.2f}')
print(f'Stop Loss  : {sl:.2f}  ({atr_h1*2.5:.2f} = 2.5xATR)')
print(f'Take Profit: {tp:.2f}  (RR 1:2)')
print('=' * 65)
mt5.shutdown()
