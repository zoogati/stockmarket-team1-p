-- CREATE AND INITIALIZE "QUOTE_ADJUST" table for the sp_quote_feed stored procedures.
 -- Install it in your team database.

-- (a) create QUOTE_ADJUST table. Actual:

	create table QUOTE_ADJUST (INSTRUMENT_ID int(11)  PRIMARY KEY,
	LAST_ASK_PRICE decimal(18,4) default 0,
	LAST_ASK_SEQ_NBR int(11) default 0,
	LAST_BID_PRICE decimal(18,4) default 0,
	LAST_BID_SEQ_NBR int(11) default 0,
	AMPLITUDE decimal(18,4) default 0,
	SWITCHPOINT int default 0,
	DIRECTION tinyint default 1);

mysql> describe QUOTE_ADJUST;
+------------------+---------------+------+-----+---------+-------+
| Field            | Type          | Null | Key | Default | Extra |
+------------------+---------------+------+-----+---------+-------+
| INSTRUMENT_ID    | int(11)       | NO   | PRI | NULL    |       |
| LAST_ASK_PRICE   | decimal(18,4) | YES  |     | 0.0000  |       |
| LAST_ASK_SEQ_NBR | int(11)       | YES  |     | 0       |       |
| LAST_BID_PRICE   | decimal(18,4) | YES  |     | 0.0000  |       |
| LAST_BID_SEQ_NBR | int(11)       | YES  |     | 0       |       |
| AMPLITUDE        | decimal(18,4) | YES  |     | 0.0000  |       |
| SWITCHPOINT      | int(11)       | YES  |     | 0       |       |
| DIRECTION        | tinyint(4)    | YES  |     | 1       |       |
+------------------+---------------+------+-----+---------+-------+
8 rows in set (0.00 sec)

--------------------------------------------------------------------------------

	-- (b) Initialize with instrument_ids from INSTRUMENT
	insert into QUOTE_ADJUST (instrument_id)  select INSTRUMENT_ID from INSTRUMENT;

  -- (c) Update AMPLITUDE by a random factor of 2
	update QUOTE_ADJUST set AMPLITUDE=(RAND()+.5);


  -- (d) Update QUOTE_ADJUST switchpoint to random function of 750 quotes
	update QUOTE_ADJUST set switchpoint=ROUND((RAND()+.5)*750);
