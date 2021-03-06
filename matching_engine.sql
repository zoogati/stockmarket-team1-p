CREATE DEFINER=`moustafa`@`%` PROCEDURE `matching_engine`(IN `instr_id` INT, IN `quote_sq_nb` INT, IN `q_time` DATETIME)
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
  DECLARE new_instrument INT(11);
  DECLARE new_quote_date DATE;
  DECLARE new_quote_seq_nbr INT(11);
  DECLARE new_trading_symbol VARCHAR(15);
  DECLARE new_quote_time DATETIME;
  DECLARE new_ask_price DECIMAL(18,4);
  DECLARE new_ask_size INT(11);
  DECLARE new_bid_price DECIMAL(18,4);
  DECLARE new_bid_size INT(11);
    
  DECLARE trade_size INT(11) DEFAULT 0;
  DECLARE trade_price DECIMAL(18,4);
  DECLARE carry_over INT(11);

  DECLARE cur1 CURSOR FOR SELECT * FROM STOCK_QUOTE_FEED
                                    WHERE INSTRUMENT_ID = instr_id
                                    AND (
										(QUOTE_SEQ_NBR < quote_sq_nb AND QUOTE_TIME <= new_quote_time) OR 
										(QUOTE_SEQ_NBR = new_quote_seq_nbr AND QUOTE_TIME < new_quote_time)	
										)
                                    AND (
                            (new_bid_price * ASK_PRICE > 0 AND ASK_PRICE <= new_bid_price) OR
                                          (new_ask_price > 0 AND BID_PRICE >= new_ask_price)
                                        )
                                      AND QUOTE_DATE = DATE(new_quote_time)
                                   ORDER BY ASK_PRICE ASC, BID_PRICE DESC, QUOTE_SEQ_NBR, QUOTE_TIME;
  
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET loop_end=1;

  SELECT  *
  INTO  new_instrument,
        new_quote_date,
        new_quote_seq_nbr ,
        new_trading_symbol,
        new_quote_time ,
        new_ask_price,
        new_ask_size,
        new_bid_price,
        new_bid_size
  FROM STOCK_QUOTE_FEED
  WHERE INSTRUMENT_ID = instr_id AND QUOTE_SEQ_NBR = quote_sq_nb AND QUOTE_TIME = q_time;

  IF new_ask_price > 0 THEN
    SET trade_price =new_ask_price; SET trade_size = new_ask_size;
  ELSE
    SET trade_price =new_bid_price; SET trade_size = new_bid_size;
  END IF;

  SET carry_over = trade_size;

	-- SELECT 'MATCHING ENGINE...';

  OPEN cur1;
    order_loop: LOOP

    FETCH cur1 INTO this_instrument,
                      this_quote_date,
                      this_quote_seq_nbr,
                      this_trading_symbol,
                      this_quote_time,
                      this_ask_price,
                      this_ask_size,
                      this_bid_price,
                      this_bid_size;

      IF (loop_end OR carry_over = 0)
        THEN LEAVE order_loop;
      END IF;

      IF new_ask_price > 0 THEN -- ask quote- find matching bids --

        IF carry_over >= this_bid_size THEN
          SET carry_over = carry_over - this_bid_size;
          DELETE FROM STOCK_QUOTE_FEED WHERE (INSTRUMENT_ID = this_instrument AND
            QUOTE_SEQ_NBR = this_quote_seq_nbr AND QUOTE_TIME = this_quote_time);

		  INSERT INTO STOCK_TRADE (INSTRUMENT_ID, TRADE_DATE, TRADE_SEQ_NBR, TRADING_SYMBOL, TRADE_TIME, TRADE_PRICE, TRADE_SIZE)
		  VALUES( new_instrument,
				  new_quote_date,
				  new_quote_seq_nbr,
				  new_trading_symbol,
				  new_quote_time,
				  this_bid_price,
				  this_bid_size);
			  
        ELSE
          SET this_bid_size = this_bid_size - carry_over; 
          UPDATE STOCK_QUOTE_FEED SET BID_SIZE = this_bid_size
          WHERE INSTRUMENT_ID = this_instrument AND QUOTE_SEQ_NBR = this_quote_seq_nbr
            AND QUOTE_TIME = this_quote_time;
            
		  INSERT INTO STOCK_TRADE (INSTRUMENT_ID, TRADE_DATE, TRADE_SEQ_NBR, TRADING_SYMBOL, TRADE_TIME, TRADE_PRICE, TRADE_SIZE)
		  VALUES( new_instrument,
				  new_quote_date,
				  new_quote_seq_nbr,
				  new_trading_symbol,
				  new_quote_time,
				  this_bid_price,
				  carry_over);
                  
		  SET carry_over = 0;
            
		  
        END IF;

      ELSE

        IF carry_over >= this_ask_size THEN
          
          SET carry_over = carry_over - this_ask_size;
          DELETE FROM STOCK_QUOTE_FEED WHERE (INSTRUMENT_ID = this_instrument AND
            QUOTE_SEQ_NBR = this_quote_seq_nbr AND QUOTE_TIME = this_quote_time);
            
		  INSERT INTO STOCK_TRADE(INSTRUMENT_ID, TRADE_DATE, TRADE_SEQ_NBR, TRADING_SYMBOL, TRADE_TIME, TRADE_PRICE, TRADE_SIZE)
		  VALUES( new_instrument,
				  new_quote_date,
				  new_quote_seq_nbr,
				  new_trading_symbol,
				  new_quote_time,
				  this_ask_price,
				  this_ask_size);
                  
        ELSE
          
          SET this_ask_size = this_ask_size - carry_over;
          UPDATE STOCK_QUOTE_FEED SET ASK_SIZE = this_ask_size
          WHERE INSTRUMENT_ID = this_instrument AND QUOTE_SEQ_NBR = this_quote_seq_nbr
            AND QUOTE_TIME = this_quote_time;
            
		  INSERT INTO STOCK_TRADE(INSTRUMENT_ID, TRADE_DATE, TRADE_SEQ_NBR, TRADING_SYMBOL, TRADE_TIME, TRADE_PRICE, TRADE_SIZE)
		  VALUES( new_instrument,
				  new_quote_date,
				  new_quote_seq_nbr,
				  new_trading_symbol,
				  new_quote_time,
				  this_ask_price,
				  carry_over);
                  
		  SET carry_over = 0;

        END IF;
      END IF;

    END LOOP;
  CLOSE cur1;
    
    
    IF carry_over != 0 THEN
        IF new_ask_price > 0 THEN
            SET new_ask_size = carry_over;
            UPDATE STOCK_QUOTE_FEED SET ASK_SIZE = new_ask_size
            WHERE INSTRUMENT_ID = new_instrument 
            AND QUOTE_TIME = new_quote_time 
            AND QUOTE_SEQ_NBR = new_quote_seq_nbr;
        ELSE
            SET new_bid_size = carry_over;
            UPDATE STOCK_QUOTE_FEED SET BID_SIZE = new_bid_size
            WHERE INSTRUMENT_ID = new_instrument 
            AND QUOTE_TIME = new_quote_time 
            AND QUOTE_SEQ_NBR = new_quote_seq_nbr;
        END IF;
                                                
	ELSE
		DELETE FROM STOCK_QUOTE_FEED WHERE 
			INSTRUMENT_ID = new_instrument 
            AND QUOTE_TIME = new_quote_time 
            AND QUOTE_SEQ_NBR = new_quote_seq_nbr;
                                            
    END IF;


	-- SET new_quote_time = DATE_ADD(NOW(), INTERVAL 0 SECOND);
    -- SET new_quote_date = DATE_ADD(NOW(), INTERVAL 0 DAY);
   /* SET trade_size = trade_size - carry_over;
    IF trade_size > 0 THEN
        INSERT INTO STOCK_TRADE
        VALUES(new_instrument,
              new_quote_date,
              new_quote_seq_nbr,
              new_trading_symbol,
              new_quote_time,
              trade_price,
              trade_size);
	END IF;*/

END