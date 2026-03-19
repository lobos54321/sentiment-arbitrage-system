#!/usr/bin/env python3
"""
从 zeabur-export.json 的 premium_signals 表解析完整指数，
输出为 /tmp/mar1618_signals.json，供回测脚本使用。
"""
import json, re
from datetime import datetime, timezone

def parse_desc(desc):
    if not desc:
        return {}
    result = {}
    patterns = {
        'super_index':     r'Super\s+Index[：:]\s*(\d+)',
        'ai_index':        r'AI\s+Index[：:]\s*(\d+)',
        'trade_index':     r'Trade\s+Index[：:]\s*(\d+)',
        'security_index':  r'Security\s+Index[：:]\s*(\d+)',
        'address_index':   r'Address\s+Index[：:]\s*(\d+)',
        'sentiment_index': r'Sentiment\s+Index[：:]\s*(\d+)',
        'media_index':     r'Media\s+Index[：:]\s*(\d+)',
    }
    for key, pat in patterns.items():
        m = re.search(pat, desc, re.IGNORECASE)
        if m:
            result[key] = int(m.group(1))
    # MC
    mc_m = re.search(r'\*\*MC[：:]\*\*\s*([\d.]+)K', desc) or re.search(r'MC[：:]\s*([\d.]+)K', desc)
    if mc_m:
        result['market_cap'] = float(mc_m.group(1)) * 1000
    # NOT_ATH / ATH
    result['is_ath'] = ('NOT_ATH' not in desc) and (
        'All Time High' in desc or 'AllTimeHigh' in desc or 'All.Time.High' in desc
    )
    return result

def main():
    data = json.load(open('data/zeabur-export.json'))
    ps_rows = data['tables']['premium_signals']['rows']

    out = []
    skip_no_ai = 0
    skip_ath   = 0

    for r in ps_rows:
        desc  = r.get('description', '')
        ca    = r.get('token_ca', '')
        sym   = r.get('symbol', '')
        ts_ms = r.get('timestamp') or r.get('created_at') or 0
        if not ts_ms or not ca:
            continue

        parsed = parse_desc(desc)
        if not parsed.get('ai_index') and 'AI' not in desc:
            skip_no_ai += 1
            continue
        if parsed.get('is_ath'):
            skip_ath += 1
            continue

        # 组装为统一格式（兼容 backtest-not-ath-v3.py）
        sig = {
            'ts':              ts_ms,            # 毫秒
            'token_ca':        ca,
            'token_name':      r.get('name') or sym,
            'symbol':          sym,
            'is_ath':          parsed.get('is_ath', False),
            'market_cap':      parsed.get('market_cap') or r.get('market_cap') or 0,
            'super_index':     parsed.get('super_index', 0),
            'ai_index':        parsed.get('ai_index', 0),
            'trade_index':     parsed.get('trade_index', 0),
            'security_index':  parsed.get('security_index', 0),
            'address_index':   parsed.get('address_index', 0),
            'sentiment_index': parsed.get('sentiment_index', 0),
            'media_index':     parsed.get('media_index', 60),
        }
        out.append(sig)

    # 按时间排序
    out.sort(key=lambda x: x['ts'])

    # 日期分布
    from collections import Counter
    dates = Counter(datetime.fromtimestamp(s['ts']/1000, tz=timezone.utc).strftime('%m/%d') for s in out)
    print(f'解析完成: {len(out)} 笔信号 (跳过 no_ai={skip_no_ai}, ath={skip_ath})')
    for d in sorted(dates):
        print(f'  {d}: {dates[d]}笔')

    with open('/tmp/mar1618_signals.json', 'w') as f:
        json.dump(out, f, separators=(',', ':'))
    print(f'已保存 -> /tmp/mar1618_signals.json')

if __name__ == '__main__':
    main()
