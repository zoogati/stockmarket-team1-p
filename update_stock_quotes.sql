CREATE DEFINER=`moustafa`@`%` PROCEDURE `update_stock_quote`(IN switch INT, IN amp DECIMAL(18,2))
BEGIN

DECLARE instr INT;
DECLARE endloop TINYINT DEFAULT 0;

DECLARE cur1 CURSOR FOR (SELECT INSTRUMENT_ID FROM INSTRUMENT ORDER BY INSTRUMENT_ID);
DECLARE CONTINUE HANDLER FOR NOT FOUND SET endloop=1;

OPEN cur1;

quote_loop: LOOP
	FETCH cur1 INTO instr;
    IF (endloop)
          THEN LEAVE quote_loop;
    END IF;
	CALL makewaves(instr, ROUND((RAND()+0.5)*switch), (RAND()+0.5)*amp);

END LOOP;

CLOSE cur1;

END