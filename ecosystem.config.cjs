module.exports = {
    apps: [{
        name: 'trader',
        script: 'src/index.js',
        cwd: '/Users/boliu/sentiment-arbitrage-system',

        // 内存上限：超过 1.5GB 自动重启
        max_memory_restart: '1500M',

        // 自动重启配置
        autorestart: true,
        watch: false,

        // 重启间隔策略：指数退避，最大10秒
        exp_backoff_restart_delay: 1000,
        max_restarts: 10,
        min_uptime: '30s',

        // 定时重启：每6小时 (0:00, 6:00, 12:00, 18:00)
        cron_restart: '0 */6 * * *',

        // 日志配置
        log_file: './logs/combined.log',
        out_file: './logs/out.log',
        error_file: './logs/error.log',
        log_date_format: 'YYYY-MM-DD HH:mm:ss',
        merge_logs: true,

        // 环境变量
        env: {
            NODE_ENV: 'production'
        },

        // 优雅关闭
        kill_timeout: 10000,
        listen_timeout: 5000
    }]
    // 注意: dashboard 由 index.js 内部启动，不需要单独进程
};
