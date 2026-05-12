module.exports = {
    apps: [
        // === Node.js 主交易系统 ===
        {
            name: 'trader',
            script: 'src/index.js',
            cwd: '/app',

            max_memory_restart: '1500M',
            autorestart: true,
            watch: false,
            exp_backoff_restart_delay: 1000,
            max_restarts: 10,
            min_uptime: '30s',
            cron_restart: '0 */6 * * *',

            log_file: './logs/combined.log',
            out_file: './logs/out.log',
            error_file: './logs/error.log',
            log_date_format: 'YYYY-MM-DD HH:mm:ss',
            merge_logs: true,

            env: {
                NODE_ENV: 'production',
                MARKET_DATA_UNIFIED_ROLLOUT: 'true',
                MARKET_DATA_UNIFIED_PREMIUM: 'true',
            },
            kill_timeout: 10000,
            listen_timeout: 5000
        },

        // === 24小时生命周期跟踪（虚拟盘） ===
        {
            name: 'lifecycle-tracker',
            script: 'scripts/lifecycle_24h_tracker.py',
            args: '--track',
            interpreter: 'python3',
            cwd: '/app',

            max_memory_restart: '512M',
            autorestart: true,
            watch: false,
            exp_backoff_restart_delay: 5000,
            max_restarts: 5,
            min_uptime: '2m',

            // 日志：PM2 自动管理
            out_file: './logs/lifecycle-out.log',
            error_file: './logs/lifecycle-error.log',
            log_date_format: 'YYYY-MM-DD HH:mm:ss',
            merge_logs: true,

            env: {
                PYTHONUNBUFFERED: '1',
                SENTIMENT_DB: '/app/data/sentiment.db',
                LIFECYCLE_DB: '/app/data/lifecycle_tracks.db',
                KLINE_DB: '/app/data/kline_cache.db',
            },
            kill_timeout: 30000
        },

        // === 实盘监控 + FBR 过滤（虚拟盘） ===
        {
            name: 'paper-trader',
            script: 'scripts/paper_trade_monitor.py',
            args: '',
            interpreter: 'python3',
            cwd: '/app',

            max_memory_restart: '512M',
            autorestart: true,
            watch: false,
            exp_backoff_restart_delay: 10000,
            max_restarts: 5,
            min_uptime: '2m',

            out_file: './logs/paper-trader-out.log',
            error_file: './logs/paper-trader-error.log',
            log_date_format: 'YYYY-MM-DD HH:mm:ss',
            merge_logs: true,

            env: {
                PYTHONUNBUFFERED: '1',
                SENTIMENT_DB: '/app/data/sentiment.db',
                PAPER_DB: '/app/data/paper_trades.db',
                KLINE_DB: '/app/data/kline_cache.db',
                PAPER_EXECUTION_PENALTY_ENABLED: 'true',
                PAPER_BUY_SLIPPAGE_BPS: '35',
                PAPER_SELL_SLIPPAGE_BPS: '50',
                PAPER_BUY_DELAY_BPS: '15',
                PAPER_SELL_DELAY_BPS: '25',
                PAPER_FEE_BPS: '8',
                MARKET_DATA_UNIFIED_ROLLOUT: 'true',
                MARKET_DATA_UNIFIED_PAPER_MONITOR: 'true',
            },
            kill_timeout: 30000
        },

        // === GMGN 独立外部 alpha shadow scout（只记录，不交易） ===
        {
            name: 'gmgn-candidate-scout',
            script: 'scripts/gmgn_candidate_scout.py',
            args: '--loop --state-db /app/data/paper_trades.db --out /app/data/gmgn_candidates.jsonl',
            interpreter: 'python3',
            cwd: '/app',

            max_memory_restart: '256M',
            autorestart: true,
            watch: false,
            exp_backoff_restart_delay: 10000,
            max_restarts: 10,
            min_uptime: '30s',

            log_file: '/app/data/gmgn-scout.log',
            out_file: '/app/data/gmgn-scout.log',
            error_file: '/app/data/gmgn-scout.log',
            log_date_format: 'YYYY-MM-DD HH:mm:ss',
            merge_logs: true,

            env: {
                PYTHONUNBUFFERED: '1',
                PAPER_DB: '/app/data/paper_trades.db',
                EXTERNAL_ALPHA_DB: '/app/data/paper_trades.db',
            },
            kill_timeout: 30000
        },

        // === 多源共振 shadow cohort collector（只归因，不交易） ===
        {
            name: 'source-resonance-shadow',
            script: 'scripts/source_resonance_shadow.py',
            args: '--loop --paper-db /app/data/paper_trades.db --signal-db /app/data/sentiment_arb.db',
            interpreter: 'python3',
            cwd: '/app',

            max_memory_restart: '256M',
            autorestart: true,
            watch: false,
            exp_backoff_restart_delay: 10000,
            max_restarts: 10,
            min_uptime: '30s',

            log_file: '/app/data/source-resonance.log',
            out_file: '/app/data/source-resonance.log',
            error_file: '/app/data/source-resonance.log',
            log_date_format: 'YYYY-MM-DD HH:mm:ss',
            merge_logs: true,

            env: {
                PYTHONUNBUFFERED: '1',
                PAPER_DB: '/app/data/paper_trades.db',
                SENTIMENT_DB: '/app/data/sentiment_arb.db',
                DB_PATH: '/app/data/sentiment_arb.db',
            },
            kill_timeout: 30000
        },
    ]
};
