/* run this after sp_increase_data to generate stock symbols for newly created instruments*/
/* DECLARATIONS AND INITIALIZATIONS */

USE stockmarket;

DELIMITER //
DROP PROCEDURE IF EXISTS update_symbol//

CREATE PROCEDURE update_symbol()
BEGIN DECLARE one char(26);
DECLARE two char(26);
DECLARE three char(26);
DECLARE four char(26);
DECLARE first_ch char(1);
DECLARE SECOND char(1);
DECLARE third char(1);
DECLARE new_symbol char(4);
DECLARE instr_count int;
DECLARE max_ids int;
DECLARE loop1 int;
DECLARE loop2 int;
DECLARE loop3 int;
DECLARE loop4 int;

SET one='ABCDEFGHIJKLMNOPQRSTUVWXYZ';
SET two='ABCDEFGHIJKLMNOPQRSTUVWXYZ';
SET three='ABCDEFGHIJKLMNOPQRSTUVWXYZ';
SET four='ABCDEFGHIJKLMNOPQRSTUVWXYZ';

SET instr_count = (SELECT min(INSTRUMENT_ID) FROM stockmarket.INSTRUMENT WHERE TRADING_SYMBOL='');

SET loop1 = 1;
SET loop2 = 1;
SET loop3 = 1;
SET loop4 = 1;

SELECT max_ids = (SELECT max(INSTRUMENT_ID) FROM stockmarket.INSTRUMENT WHERE TRADING_SYMBOL='' );

 -- First half of the nested loops. - Note leave statement

firstloop: WHILE loop1 <= 26 DO
  IF instr_count IS NULL THEN
   leave firstloop;
  END IF;

  SET first_ch=substring(one,loop1,1);

    secondloop: WHILE loop2 <=26 DO
      SET SECOND=substring(two,loop2,1);

      thirdloop: WHILE loop3 <=26 DO
        SET third=substring(three,loop3,1);

        fourthloop: WHILE loop4 <=26 DO

          SET new_symbol=concat(first_ch,SECOND,third,substring(four,loop4,1));
          SELECT new_symbol;
          UPDATE stockmarket.INSTRUMENT
          SET TRADING_SYMBOL=new_symbol,
              INSTR_NAME = new_symbol,
              INSTR_DESC =new_symbol
          WHERE INSTRUMENT_ID = instr_count;
          SET instr_count = instr_count + 1;

          IF instr_count > max_ids THEN
            leave fourthloop;
          END IF;

          SET loop4 = loop4 + 1;
        END WHILE;

        COMMIT;

  --Rest of nested loop, propagate changes to INSTRUMENT to other tables

        IF instr_count > max_ids THEN
            leave thirdloop;
        END IF;
        SET loop4 = 1;
        SET loop3 = loop3 + 1;
      END WHILE;

      IF instr_count > max_ids THEN
        leave secondloop;
      END IF;

      SET loop3 = 1;
      SET loop2 = loop2 + 1;
    END WHILE;

    IF instr_count > max_ids THEN
      leave firstloop;
    END IF;
    SET loop2 = 1;
    SET loop1 = loop1 + 1;
END WHILE;

SELECT   instr_count,
         max_ids,
         new_symbol,
         loop1,
         loop2,
         loop3;
COMMIT;

UPDATE STOCK_HISTORY
SET trading_symbol=
  (SELECT trading_symbol
   FROM INSTRUMENT
   WHERE STOCK_HISTORY.instrument_id=INSTRUMENT.instrument_id);
COMMIT;

UPDATE stockmarket.STOCK_QUOTE
SET TRADING_SYMBOL =
  (SELECT trading_symbol
   FROM INSTRUMENT
   WHERE STOCK_QUOTE.instrument_id=INSTRUMENT.instrument_id);
COMMIT;

UPDATE stockmarket.STOCK_TRADE
SET TRADING_SYMBOL =
  (SELECT trading_symbol
   FROM INSTRUMENT
   WHERE STOCK_TRADE.instrument_id=INSTRUMENT.instrument_id);
COMMIT;

END//
DELIMITER ;
