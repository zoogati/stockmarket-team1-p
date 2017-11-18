CREATE DEFINER=`moustafa`@`%` PROCEDURE `scan_feed`()
BEGIN

DECLARE this_instrument INT(11);
  DECLARE this_quote_date DATE;
  DECLARE this_quote_seq_nbr INT(11);
  DECLARE this_trading_symbol VARCHAR(15);
  DECLARE this_quote_time DATETIME;
  DECLARE this_ask_price DECIMAL(18,4);
  DECLARE this_ask_size INT(11);
  DECLARE this_bid_price DECIMAL(18,4);
  DECLARE this_bid_size INT(11);
  DECLARE loop_end INT DEFAULT FALSE;

  DECLARE scan CURSOR FOR SELECT * FROM STOCK_QUOTE_FEED
									WHERE INSTRUMENT_ID IN (SELECT INSTRUMENT_ID FROM INSTRUMENT)
									ORDER BY QUOTE_SEQ_NBR, QUOTE_TIME;
  
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET loop_end=1;
  
  OPEN scan;
	quote_loop: LOOP
		
        FETCH scan INTO this_instrument,
                      this_quote_date,
                      this_quote_seq_nbr,
                      this_trading_symbol,
                      this_quote_time,
                      this_ask_price,
                      this_ask_size,
                      this_bid_price,
                      this_bid_size;
		 
         IF (loop_end)
			THEN LEAVE quote_loop;
		  END IF;
		
        CALL matching_engine(this_instrument, this_quote_seq_nbr, this_quote_time);
  
	END LOOP;
  CLOSE scan;


END