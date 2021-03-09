-- Calculate VWAP over aggregation window by minute
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

-- JOIN trades with var99 stream
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

-- Generate 2 records (buyer, seller) per trade record
CREATE VIEW trades_entity AS (
    SELECT
	event_time,
	symbol,
	price,
	vol,
	buyer_id,
	seller_id,
	IF (entity_id = buyer_id, vol, 0) AS buyer_vol,
	IF (entity_id = seller_id, vol, 0) AS seller_vol,
	entity_id
    FROM
	trades
    CROSS JOIN UNNEST (
        ARRAY [ buyer_id, seller_id ]
    ) AS T (entity_id)
);

    SELECT
	symbol,
	entity_id,
	COUNT (1)                                    AS trade_cnt,
        SUM (buyer_vol - seller_vol)                 AS net_vol,
	SUM (seller_vol * price - buyer_vol * price) AS net_balance
    FROM
        trades_entity
    GROUP BY symbol, entity_id
;
