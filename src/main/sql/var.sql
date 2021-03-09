-- Grab previous price
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

-- Extract and fill sampled timestamps with help of UDTFs

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

-- Grab previous mid_price from last sampled row so we can calculate returns
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

-- Calculate VAR:
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
	    AVG (mid_price)                                    OVER lookback AS avg_price,
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
