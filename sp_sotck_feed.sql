
DELIMITER //
DROP PROCEDURE IF EXISTS sp_quote_feed //
/* first try just input to output just loop count*/
CREATE PROCEDURE sp_quote_feed(IN loops int)
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
DECLARE loopcount int(11);
DECLARE maxloops int(11);
/*variables for stockmarket.QUOTE_ADJUST values*/
DECLARE qa_last_ask_price decimal(18,4);
DECLARE qa_last_ask_seq_nbr int(11);
DECLARE qa_last_bid_price decimal(18,4);
DECLARE qa_last_bid_seq_nbr int(11);
DECLARE qa_amplitude decimal(18,4);
DECLARE qa_switchpoint int(11);
DECLARE qa_direction tinyint;
DECLARE db_done int
DEFAULT FALSE;
DECLARE cur1 CURSOR FOR SELECT * FROM stockmarket.STOCK_QUOTE
                                USE INDEX FOR ORDER BY (XK2_STOCK_QUOTE, XK4_STOCK_QUOTE)
                                ORDER BY QUOTE_SEQ_NBR, QUOTE_TIME;

DECLARE CONTINUE HANDLER FOR NOT FOUND SET db_done=1;
SET maxloops=loops*1000;
SET loopcount=1;

OPEN cur1;
quote_loop: LOOP
  IF (db_done OR loopcount=maxloops) THEN
    leave quote_loop;
  END IF;
  FETCH cur1 INTO this_instrument,
                  this_quote_date,
                  this_quote_seq_nbr,
                  this_trading_symbol,
                  this_quote_time,
                  this_ask_price,
                  this_ask_size,
                  this_bid_price,
                  this_bid_size;

                  /*all update logic goes here...first get stockmarket.QUOTE_ADJUST values into variables*/

SELECT LAST_ASK_PRICE,
       LAST_ASK_SEQ_NBR,
       LAST_BID_PRICE,
       LAST_BID_SEQ_NBR,
       AMPLITUDE,
       SWITCHPOINT,
       DIRECTION INTO qa_last_ask_price,
                      qa_last_ask_seq_nbr,
                      qa_last_bid_price,
                      qa_last_bid_seq_nbr,
                      qa_amplitude,
                      qa_switchpoint,
                      qa_direction
FROM stockmarket.QUOTE_ADJUST
WHERE INSTRUMENT_ID=this_instrument; IF this_ask_price > 0 THEN /* it is an ask*/
  UPDATE stockmarket.QUOTE_ADJUST
  SET LAST_ASK_PRICE=this_ask_price
  WHERE INSTRUMENT_ID=this_instrument;
    UPDATE stockmarket.QUOTE_ADJUST
    SET LAST_ASK_SEQ_NBR=this_quote_seq_nbr WHERE INSTRUMENT_ID=this_instrument; IF qa_last_ask_price > 0 THEN /*not first ask for this inst*/
    SET this_ask_price=qa_last_ask_price+(ABS(this_ask_price-qa_last_ask_price)*qa_amplitude*qa_direction); END IF; ELSE /*it is a bid*/
    UPDATE stockmarket.QUOTE_ADJUST
    SET LAST_BID_PRICE=this_bid_price WHERE INSTRUMENT_ID=this_instrument;
    UPDATE stockmarket.QUOTE_ADJUST
    SET LAST_BID_SEQ_NBR=this_quote_seq_nbr WHERE INSTRUMENT_ID=this_instrument; IF qa_last_bid_price > 0 THEN /*not first bid for this inst*/
    SET this_bid_price=qa_last_bid_price+(ABS(this_bid_price-qa_last_bid_price)*qa_amplitude*qa_direction); END IF; END IF; /* end if this is an ask or a bid*/ /* in all cases check and reset switchpoint if needed reset amplitude and update dates*/ IF qa_switchpoint > 0 THEN
  UPDATE stockmarket.QUOTE_ADJUST
  SET SWITCHPOINT=SWITCHPOINT-1
  WHERE INSTRUMENT_ID=this_instrument ; ELSE /*switchpoint <=0, recalculate switchpoint and change direction */
    UPDATE stockmarket.QUOTE_ADJUST
    SET SWITCHPOINT=ROUND((RAND()+.5)*400),
        DIRECTION=DIRECTION*-1 WHERE INSTRUMENT_ID=this_instrument; END IF;
  UPDATE stockmarket.QUOTE_ADJUST
  SET AMPLITUDE=(RAND()+.5) WHERE INSTRUMENT_ID=this_instrument;
  SET this_quote_date=DATE_ADD(this_quote_date, INTERVAL 12 YEAR);
  SET this_quote_time=DATE_ADD(this_quote_time, INTERVAL 12 YEAR); /* now write out the record*/
  INSERT INTO stockmarket.STOCK_QUOTE_FEED
VALUES(this_instrument,
       this_quote_date,
       this_quote_seq_nbr,
       this_trading_symbol,
       this_quote_time,
       this_ask_price,
       this_ask_size,
       this_bid_price,
       this_bid_size);
SET loopcount=loopcount+1; END LOOP; CLOSE cur1; END //
DELIMITER ;
