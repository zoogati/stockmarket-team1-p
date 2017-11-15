--This sp updates direction and amplitude of quotes in place, per instrument.
-- First the parms, declarations and initializations

DELIMITER //
DROP PROCEDURE IF EXISTS sp_quote_makewaves //

CREATE PROCEDURE sp_quote_makewaves (IN tr_symbol varchar(15), IN switchpoint int ,IN amp decimal(18,2))

BEGIN

/*declarations*/

DECLARE loopcount int;
DECLARE rand_switchpoint int;
DECLARE quotecount int;
DECLARE quoteseq int;
DECLARE direction tinyint;
DECLARE lastbid decimal(18,2);
DECLARE thisbid decimal(18,2);
DECLARE alt_thisbid decimal(18,2);
DECLARE alt_lastbid decimal(18,2);
DECLARE bid_change decimal(18,2);
DECLARE lastask decimal(18,2);
DECLARE thisask decimal(18,2);
DECLARE alt_thisask decimal(18,2);
DECLARE alt_lastask decimal(18,2);
DECLARE ask_change decimal(18,2);

/*initialize*/

SET loopcount=0;
SET rand_switchpoint=0;
SET quotecount=0;
SET quoteseq=0;
SET direction=1;
SET lastbid=0;
SET thisbid=0;
SET alt_thisbid=0;
SET alt_lastbid=0;
SET bid_change=0;
SET lastask=0;
SET thisask=0;
SET alt_thisask=0;
SET alt_lastask=0;
SET ask_change=0;

-- Update the quote based on change and direction – start with bids.
-- Note select into, if-then-else end if construction

SELECT count(*)
  INTO quotecount
  FROM STOCK_QUOTE
  WHERE TRADING_SYMBOL=tr_symbol;

SELECT "quotes to process",
       quotecount;

       WHILE loopcount < quotecount DO
       /*main loop*/

          SELECT BID_PRICE
          INTO thisbid
          FROM STOCK_QUOTE
          WHERE TRADING_SYMBOL=tr_symbol
            AND YEAR(QUOTE_TIME)<2016
            AND BID_PRICE>0
          LIMIT 1;

          SELECT QUOTE_SEQ_NBR INTO quoteseq
          FROM STOCK_QUOTE
          WHERE TRADING_SYMBOL=tr_symbol
            AND YEAR(QUOTE_TIME)<2016
            AND BID_PRICE> 0
          LIMIT 1;

          IF lastbid >0 THEN
            SET bid_change=ABS(thisbid-lastbid);
            SET alt_thisbid=alt_lastbid+(bid_change*direction*amp);
            SET alt_lastbid=alt_thisbid;
            SET lastbid=thisbid;

              /*select "bid change",bid_change;*/

          ELSE                      /*first read*/
            SET lastbid=thisbid;
            SET alt_lastbid=thisbid;
            SET alt_thisbid=thisbid;
          END IF;

          UPDATE STOCK_QUOTE
          SET QUOTE_DATE=DATE_ADD(QUOTE_DATE, INTERVAL 11 YEAR),
              QUOTE_TIME=DATE_ADD(QUOTE_TIME, INTERVAL 11 YEAR),
              BID_PRICE=alt_thisbid
          WHERE TRADING_SYMBOL=tr_symbol
            AND QUOTE_SEQ_NBR=quoteseq
            AND BID_PRICE > 0;

          COMMIT;

          -- Now process the -asks- and update amplitude and direction at the end of each iteration

          SELECT ASK_PRICE INTO thisask
          FROM STOCK_QUOTE
          WHERE TRADING_SYMBOL=tr_symbol
            AND YEAR(QUOTE_TIME)<2016
            AND ASK_PRICE>0
          LIMIT 1;

          SELECT QUOTE_SEQ_NBR INTO quoteseq
          FROM STOCK_QUOTE
          WHERE TRADING_SYMBOL=tr_symbol
            AND YEAR(QUOTE_TIME)<2016
            AND ASK_PRICE> 0
          LIMIT 1;

          IF lastask >0 THEN
            SET ask_change=ABS(thisask-lastask);
            SET alt_thisask=alt_lastask+(ask_change*direction*amp);
            SET alt_lastask=alt_thisask;
            SET lastask=thisask;

            /*select "ask change", ask_change;*/

            ELSE /*first read*/
              SET lastask=thisask;
              SET alt_lastask=thisask;
              SET alt_thisask=thisask;
            END IF;

          UPDATE STOCK_QUOTE
          SET QUOTE_DATE=DATE_ADD(QUOTE_DATE, INTERVAL 11 YEAR),
              QUOTE_TIME=DATE_ADD(QUOTE_TIME, INTERVAL 11 YEAR),
              ASK_PRICE=alt_thisask
          WHERE TRADING_SYMBOL=tr_symbol
            AND QUOTE_SEQ_NBR=quoteseq
            AND ASK_PRICE > 0;

          COMMIT;

          SET loopcount=loopcount+1;
          SET rand_switchpoint=ROUND((RAND()+.5)*switchpoint);

          IF loopcount%rand_switchpoint=0 THEN
            SET direction=direction*-1;
            SELECT "direction changed",
                   rand_switchpoint,
                   direction,
                   loopcount;
          END IF;

       END WHILE;
END //
DELIMITER ;
