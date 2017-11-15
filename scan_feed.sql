DELIMITER $$
CREATE DEFINER=`moustafa`@`%` PROCEDURE `scan_feed`()
BEGIN

DECLARE this_instrument int(11);
  DECLARE this_quote_date date;
  DECLARE this_quote_seq_nbr int(11);
  DECLARE this_trading_symbol varchar(15);
  DECLARE this_quote_time datetime;
  DECLARE this_ask_price decimal(18,4);
  DECLARE this_ask_size int(11);
  DECLARE this_bid_price decimal(18,4);
  DECLARE this_bid_size int(11);
  DECLARE loop_end int DEFAULT FALSE;

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
			THEN leave quote_loop;
		  END IF;
		
        call matching_engine(this_instrument, this_quote_seq_nbr, this_quote_time);
  
	END LOOP;
  CLOSE scan;


END$$
DELIMITER ;
