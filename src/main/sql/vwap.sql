-- Create trades table
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

-- Live Intraday VWAP
SELECT
  symbol,
  SUM (vol)                     AS cumulative_volume,
  SUM (price * vol)             AS cumulative_pv,
  SUM (price * vol) / SUM (vol) AS vwap
FROM
  trades
GROUP BY
  symbol
;

-- "Live" replay
CREATE FUNCTION replay_after AS 'varstream.ReplayAfterFunction' LANGUAGE JAVA ;

CREATE VIEW trades_replay AS (
    SELECT * FROM trades
    LEFT JOIN LATERAL TABLE (replay_after (120, trades.event_time)) ON TRUE
) ;

SELECT * FROM trades_replay ;

SELECT
  symbol,
  SUM (vol)                     AS cumulative_volume,
  SUM (price * vol)             AS cumulative_pv,
  SUM (price * vol) / SUM (vol) AS vwap
FROM
  trades_replay
GROUP BY
  symbol
;

-- 1 minute window VWAP
CREATE VIEW vwap_1m AS (
    SELECT
	symbol,
	TUMBLE_START (event_time, INTERVAL '1' MINUTES) AS start_time,
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

-- 5 minute sliding window VWAP (1 minute increments)
CREATE VIEW vwap_5m AS (
    SELECT
	symbol,
	HOP_START (event_time, INTERVAL '1' MINUTES, INTERVAL '5' MINUTES) AS start_time,
	HOP_ROWTIME (event_time, INTERVAL '1' MINUTES, INTERVAL '5' MINUTES) AS row_time,
	MAX (price)          AS max_price,
	MIN (price)          AS min_price,
	SUM (price * vol)    AS total_price,
	SUM (vol)            AS total_vol,
	SUM (price * vol) / SUM (vol) AS vwap
    FROM
	trades
    GROUP BY
	HOP (event_time, INTERVAL '1' MINUTES, INTERVAL '5' MINUTES), symbol
);
