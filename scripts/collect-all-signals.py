#!/usr/bin/env python3
"""
全信号数据收集器
================
尝试所有可能的数据源，获取最多的NOT_ATH信号

优先级:
  1. Telegram MTProto (TELEGRAM_API_ID + TELEGRAM_API_HASH + TELEGRAM_SESSION)
  2. Zeabur export (DASHBOARD_TOKEN)  
  3. 本地日志解析 (从logs/*.gz提取)
  4. 使用现有42个ATH信号（兜底）

运行: python3 scripts/collect-all-signals.py
"""

import os, json, sys, gzip, re, datetime

HISTORY_FILE = 'data/channel-history.json'
OHLCV_CACHE  = 'data/ohlcv-cache.json'

def try_parse_logs():
    """从本地日志文件解析信号"""
    import glob
    
    log_files = sorted(glob.glob('logs/*.gz') + glob.glob('logs/*.log'))
    signals = []
    
    # pump.fun CA正则
    ca_re = re.compile(r'\b([1-9A-HJ-NP-Za-km-z]{32,44}pump)\b')
    
    for logf in log_files:
        try:
            if logf.endswith('.gz'):
                with gzip.open(logf, 'rb') as f:
                    content = f.read().decode('utf-8', errors='replace')
            else:
                with open(logf) as f:
                    content = f.read()
            
            # 找NOT_ATH_V17相关行
            for line in content.split('\n'):
                if 'NOT_ATH_V17' in line or 'New Trending' in line:
                    cas = ca_re.findall(line)
                    for ca in cas:
                        # 尝试找时间戳
                        ts_match = re.search(r'(\d{13})', line)
                        ts = int(ts_match.group(1)) if ts_match else int(datetime.datetime.now().timestamp() * 1000)
                        
                        # 找symbol
                        sym_match = re.search(r'\$(\w+)', line)
                        sym = sym_match.group(1) if sym_match else 'UNKNOWN'
                        
                        signals.append({
                            'ts': ts,
                            'type': 'NEW_TRENDING',
                            'is_ath': False,
                            'symbol': sym,
                            'token_ca': ca,
                            'market_cap': 0,
                            'source': logf,
                        })
        except Exception as e:
            pass
    
    # 去重
    seen = set()
    unique = []
    for s in signals:
        if s['token_ca'] not in seen:
            seen.add(s['token_ca'])
            unique.append(s)
    
    return unique

def main():
    print('='*60)
    print('全信号数据收集器')
    print('='*60)
    
    # 检查凭证
    tg_ready = all([
        os.environ.get('TELEGRAM_API_ID'),
        os.environ.get('TELEGRAM_API_HASH'),
        os.environ.get('TELEGRAM_SESSION'),
    ])
    zeabur_ready = bool(os.environ.get('DASHBOARD_TOKEN'))
    
    print(f'\n📋 可用数据源:')
    print(f'   {"✅" if tg_ready else "❌"} Telegram MTProto (TELEGRAM_API_ID/HASH/SESSION)')
    print(f'   {"✅" if zeabur_ready else "❌"} Zeabur export (DASHBOARD_TOKEN)')
    print(f'   📁 本地日志解析')
    
    signals_found = []
    
    if tg_ready:
        print('\n🔌 使用 Telegram MTProto 获取历史信号...')
        os.system('node scripts/fetch-channel-history.mjs')
        if os.path.exists(HISTORY_FILE):
            data = json.load(open(HISTORY_FILE))
            signals_found = data.get('signals', [])
            print(f'✅ 获取 {len(signals_found)} 个信号')
    
    elif zeabur_ready:
        print('\n🔌 使用 Zeabur export...')
        os.system(f'DASHBOARD_TOKEN={os.environ["DASHBOARD_TOKEN"]} node scripts/export-zeabur-signals.mjs')
        if os.path.exists(HISTORY_FILE):
            data = json.load(open(HISTORY_FILE))
            signals_found = data.get('signals', [])
            print(f'✅ 获取 {len(signals_found)} 个信号')
    
    else:
        print('\n📁 解析本地日志...')
        log_signals = try_parse_logs()
        print(f'   本地日志找到 {len(log_signals)} 个NOT_ATH信号')
        
        if log_signals:
            signals_found = log_signals
        else:
            print('   ⚠️  本地日志无NOT_ATH数据')
            print()
            print('='*60)
            print('❌ 无法获取NOT_ATH信号数据')
            print('='*60)
            print()
            print('需要以下凭证之一:')
            print()
            print('方案A: Telegram MTProto (推荐)')
            print('  1. 访问 https://my.telegram.org 申请API')
            print('     → 创建应用，获取 api_id 和 api_hash')
            print('  2. 运行: node scripts/authenticate-telegram.js')  
            print('     → 获取 session string')
            print('  3. 在 .env 中添加:')
            print('     TELEGRAM_API_ID=你的api_id')
            print('     TELEGRAM_API_HASH=你的api_hash')
            print('     TELEGRAM_SESSION=你的session')
            print()
            print('方案B: Zeabur Dashboard Token')
            print('  在 Zeabur 控制台 → 服务 → 环境变量中找 DASHBOARD_TOKEN')
            print('  export DASHBOARD_TOKEN=你的token')
            print('  python3 scripts/collect-all-signals.py')
            print()
            print('获取凭证后，运行:')
            print('  python3 scripts/collect-all-signals.py  # 重新收集')
            print('  python3 scripts/backtest-not-ath.py     # 运行完整回测')
            sys.exit(1)
    
    # 显示统计
    if signals_found:
        ath = [s for s in signals_found if s.get('is_ath')]
        not_ath = [s for s in signals_found if not s.get('is_ath')]
        print(f'\n📊 信号统计:')
        print(f'   ATH信号:     {len(ath)}个')
        print(f'   NOT_ATH信号: {len(not_ath)}个')
        print(f'   总计:        {len(signals_found)}个')
        
        # 保存
        if not os.path.exists(HISTORY_FILE):
            json.dump({
                'fetched_at': datetime.datetime.now().isoformat(),
                'source': 'local_logs',
                'total': len(signals_found),
                'ath_signals': len(ath),
                'trending_signals': len(not_ath),
                'signals': signals_found,
            }, open(HISTORY_FILE, 'w'), indent=2)
        
        print(f'\n🎯 下一步: python3 scripts/backtest-not-ath.py')

if __name__ == '__main__':
    main()

