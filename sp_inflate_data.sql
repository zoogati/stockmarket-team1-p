-- sp_increase_data inflates stockmarket data

USE stockmarket;

DELIMITER //

DROP PROCEDURE IF EXISTS sp_increase_data //

CREATE PROCEDURE sp_increase_data (IN id_start int , IN id_stop int)
BEGIN
DECLARE jloop int;
DECLARE sec_id int;
SET jloop = id_start;
WHILE jloop < id_stop DO
  SET sec_id = (SELECT max(INSTRUMENT_ID) FROM INSTRUMENT);
  SELECT sec_id;
  IF sec_id < jloop THEN
    INSERT INSTRUMENT
    SELECT
      INSTRUMENT_ID + jloop,
      INSTR_TYPE_ID,
      CURRENCY_ID,
      MAJOR_IDST_CLS_ID,
      SCND_IDST_CLS_ID,
      GEO_GROUP_ID,
      COUNTRY_ID,
      CAPITALIZATION_ID,
      '',
      CUSIP_NUMBER,
      concat('Financial security: ',INSTRUMENT_ID + jloop),
      concat('Financial security: ',INSTRUMENT_ID + jloop),
      ISSUED_DATE
    FROM INSTRUMENT
    WHERE INSTRUMENT_ID <=999;
      COMMIT;
  END IF;

   -- Then goes through all major tables repeating same info for different instrument ids

  SET sec_id = (SELECT max(INSTRUMENT_ID) FROM DIVIDEND_EVENT);
  IF sec_id < jloop THEN
    INSERT DIVIDEND_EVENT
    SELECT
           INSTRUMENT_ID + jloop,
           DISBURSED_DATE,
           ANNOUNCED_DATE,
           DIVIDEND_VALUE,
           MOP_INDICATOR
    FROM DIVIDEND_EVENT
    WHERE INSTRUMENT_ID <=999;
      COMMIT;
  END IF;

  SET sec_id = (SELECT max(INSTRUMENT_ID) FROM SPLIT_EVENT);
  IF sec_id < jloop THEN
    INSERT SPLIT_EVENT
    SELECT
           INSTRUMENT_ID + jloop,
           EFFECTIVE_DATE,
           ANNOUNCED_DATE,
           SPLIT_FACTOR
    FROM SPLIT_EVENT
    WHERE INSTRUMENT_ID <=999;
      COMMIT;
  END IF;

  SET sec_id = (SELECT max(INSTRUMENT_ID) FROM STOCK_HISTORY);
  IF sec_id < jloop THEN
    INSERT STOCK_HISTORY
    SELECT
           INSTRUMENT_ID + jloop,
           TRADE_DATE,
           '', -- Symbol
           OPEN_PRICE,
           CLOSE_PRICE,
           LOW_PRICE,
           HIGH_PRICE,
           VOLUME
    FROM STOCK_HISTORY
    WHERE INSTRUMENT_ID <=999;
      COMMIT;
  END IF;


      -- Notice the if-then-end if and while loop constructs

  SET sec_id = (SELECT max(INSTRUMENT_ID) FROM STOCK_QUOTE);
  IF sec_id < jloop THEN
    INSERT STOCK_QUOTE
    SELECT INSTRUMENT_ID + jloop ,
           QUOTE_DATE ,
           QUOTE_SEQ_NBR,
           '', -- Symbol
           QUOTE_TIME ,
           ASK_PRICE ,
           ASK_SIZE ,
           BID_PRICE ,
           BID_SIZE
    FROM STOCK_QUOTE
    WHERE INSTRUMENT_ID <=999;
      COMMIT;
  END IF;

  SET sec_id = (SELECT max(INSTRUMENT_ID) FROM STOCK_TRADE);
  IF sec_id < jloop THEN
    INSERT STOCK_TRADE
    SELECT INSTRUMENT_ID + jloop ,
           TRADE_DATE ,
           TRADE_SEQ_NBR ,
           '', -- Symbol
           TRADE_TIME ,
           TRADE_PRICE ,
           TRADE_SIZE
    FROM STOCK_TRADE
    WHERE INSTRUMENT_ID <=999;
      COMMIT;
  END IF;

  SET jloop = jloop + 1000;

END WHILE;
END//
DELIMITER ;
