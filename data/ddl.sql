
-- 1. Create L1 table
CREATE TABLE l1 (
    event_time     TIMESTAMP(3),
    symbol         STRING,
    best_bid_prc   DOUBLE,
    best_bid_vol   INT,
    tot_bid_vol    INT,
    num            INT,
    sym2           STRING,
    best_ask_prc   DOUBLE,
    best_ask_vol   INT,
    tot_ask_vol    INT,
    num2           INT,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECONDS
) WITH (
    'connector' = 'filesystem',
    'path' = '/Users/patrick/Documents/workspace/varstream/data/l1_raw',
    'format' = 'csv'
) ;

-- 2. Extract effective start and end times for each row (by looking at previous row)
CREATE VIEW l1_times AS
    SELECT
	symbol,
	MIN (event_time) OVER w AS start_time,
	CAST (event_time AS TIMESTAMP) AS end_time,
	FIRST_VALUE (best_bid_prc) OVER w AS bid_price,
	FIRST_VALUE (best_ask_prc) OVER w AS ask_price
    FROM l1
    WINDOW w AS (
	PARTITION BY symbol
	ORDER BY event_time
	ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    )
;

-- 3. Extract and fill sampled timestamps with help of UDTFs
CREATE FUNCTION fill_sample_per_day    AS 'varstream.FillSample$PerDayFunction'    LANGUAGE JAVA ;
CREATE FUNCTION fill_sample_per_hour   AS 'varstream.FillSample$PerHourFunction'   LANGUAGE JAVA ;
CREATE FUNCTION fill_sample_per_minute AS 'varstream.FillSample$PerMinuteFunction' LANGUAGE JAVA ;
CREATE FUNCTION fill_sample_per_second AS 'varstream.FillSample$PerSecondFunction' LANGUAGE JAVA ;

CREATE VIEW l1_sample AS
    SELECT
	symbol,
	start_time,
	end_time,
	sample_time,
	bid_price,
	ask_price,
	(bid_price + ask_price) / 2 AS mid_price
    FROM l1_times AS l1
    INNER JOIN LATERAL TABLE (fill_sample_per_minute (l1.start_time, l1.end_time, 1))
      AS T(sample_time) ON TRUE
;

-- 4. Grab previous mid_price from last sampled row so we can calculate returns
CREATE VIEW l1_sample_prev AS
    SELECT
	symbol,
	start_time,
	sample_time,
	mid_price,
	FIRST_VALUE (mid_price) OVER w AS prev_price
    FROM l1_sample
    WINDOW w AS (
	PARTITION BY symbol
	ORDER BY start_time
	ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    )
;

-- 5. Calculate VAR:
--      pct_return    = returns relative to previous row
--      avg_return    = average return over lookback period
--      stddev_return = std deviation of returns over lookback period
--      var99_return  = 99% VAR
--      var99_price   = 99% VAR against this row's mid_price
CREATE VIEW l1_var99 AS
    SELECT
        *,
	avg_return - 2.58 * stddev_return AS var99_return,
	mid_price * (1 + (avg_return - 2.58 * stddev_return)) AS var99_price
    FROM (
	SELECT
	    symbol,
	    start_time,
	    sample_time,
	    mid_price,
	    (mid_price - prev_price) / prev_price              AS pct_return,
	    AVG ((mid_price - prev_price) / prev_price)        OVER lookback AS avg_return,
	    STDDEV_POP ((mid_price - prev_price) / prev_price) OVER lookback AS stddev_return
	FROM l1_sample_prev
	WINDOW lookback AS (
	    PARTITION BY symbol
	    ORDER BY start_time
	    ROWS BETWEEN 120 PRECEDING AND CURRENT ROW
	)
    )
;

-- 6. Create trades table
CREATE TABLE trades (
    symbol         STRING,
    price          DOUBLE,
    vol            INT,
    bid_id         STRING,
    ask_id         STRING,
    buyer_id       STRING,
    seller_id      STRING,
    step           INT,
    ts_str         STRING,
    event_time     AS TO_TIMESTAMP (ts_str, 'dd-MMM-yyyy HH:mm:ss.SSS'),
    WATERMARK FOR event_time AS event_time - INTERVAL '1' MINUTE
) WITH (
    'connector' = 'filesystem',
    'path' = '/Users/patrick/Documents/workspace/varstream/data/trades_raw',
    'format' = 'csv'
);


-- 7. Calculate VWAP over aggregation window (here done by minute)
CREATE VIEW trades_sample AS (
    SELECT
	symbol,
	TUMBLE_START (event_time, INTERVAL '1' MINUTES) AS sample_time,
	TUMBLE_ROWTIME (event_time, INTERVAL '1' MINUTES) AS row_time,
	MAX (price)          AS max_price,
	MIN (price)          AS min_price,
	SUM (price * vol)    AS total_price,
	SUM (vol)            AS total_vol,
	SUM (price * vol) / SUM (vol) AS vwap
    FROM
	trades
    GROUP BY
	TUMBLE (event_time, INTERVAL '1' MINUTES), symbol
);

-- 8. JOIN trades with var99 stream
CREATE VIEW var99_trades AS (
    SELECT
	 t.symbol,
	 t.sample_time AS trade_time,
	 m.sample_time AS market_time,
	 m.var99_price,
	 t.vwap,
	 t.total_vol
    FROM l1_var99 AS m
    JOIN trades_sample AS t
    ON t.symbol = m.symbol
    AND t.sample_time = m.sample_time
);

