-- Daily average stock prices
CREATE VIEW IF NOT EXISTS daily_avg_prices AS
SELECT 
    date,
    symbol,
    AVG(open) AS avg_open,
    AVG(high) AS avg_high,
    AVG(low) AS avg_low,
    AVG(close) AS avg_close,
    SUM(volume) AS total_volume
FROM daily_stock_prices
GROUP BY date, symbol
ORDER BY date DESC, symbol;

-- Stock performance metrics (note: SQLite has limited window functions)
CREATE VIEW IF NOT EXISTS stock_performance AS
SELECT
    t1.date,
    t1.symbol,
    t1.close,
    t2.close AS prev_day_close,
    (t1.close - t2.close) / t2.close * 100 AS daily_change_pct
FROM daily_stock_prices t1
LEFT JOIN daily_stock_prices t2 ON 
    t1.symbol = t2.symbol AND 
    date(t1.date, '-1 day') = t2.date
WHERE t2.close IS NOT NULL
ORDER BY t1.date DESC, t1.symbol;

-- Monthly volatility metrics
CREATE VIEW IF NOT EXISTS stock_volatility AS
SELECT
    symbol,
    strftime('%Y-%m', date) AS month,
    AVG(high - low) AS avg_daily_range,
    AVG(volume) AS avg_volume,
    MAX(volume) AS max_volume
FROM daily_stock_prices
GROUP BY symbol, month
ORDER BY month DESC, symbol;