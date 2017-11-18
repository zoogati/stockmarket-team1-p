CREATE DEFINER=`socrates`@`%` PROCEDURE `makewaves`(IN instr INT , IN switchpoint INT ,IN amp DECIMAL(18,2))
BEGIN

/*declarations*/
	DECLARE loopcount INT;
	DECLARE rand_switchpoint INT;
	DECLARE quotecount INT;
	DECLARE quoteseq INT;
	DECLARE direction TINYINT;
	DECLARE lastbid DECIMAL(18,2);
	DECLARE thisbid DECIMAL(18,2);
	DECLARE alt_thisbid DECIMAL(18,2);
	DECLARE alt_lastbid DECIMAL(18,2);
	DECLARE bid_change DECIMAL(18,2);
	DECLARE lastask DECIMAL(18,2);
	DECLARE thisask DECIMAL(18,2);
	DECLARE alt_thisask DECIMAL(18,2);
	DECLARE alt_lastask DECIMAL(18,2);
	DECLARE ask_change DECIMAL(18,2);
	DECLARE ask_count INT; DECLARE bid_count INT;
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


	SELECT
    COUNT(*)
    INTO bid_count
    FROM STOCK_QUOTE
    WHERE INSTRUMENT_ID = instr AND BID_PRICE>0;


	SELECT
    COUNT(*)
    INTO ask_count
    FROM STOCK_QUOTE
    WHERE INSTRUMENT_ID = instr AND ASK_PRICE >0;


    SET quotecount = ask_count + bid_count;

    SELECT 'quotes to process', quotecount;

    WHILE loopcount < quotecount DO  /*main loop*/

        SELECT BID_PRICE, QUOTE_SEQ_NBR INTO thisbid, quoteseq FROM STOCK_QUOTE
        WHERE INSTRUMENT_ID=instr AND YEAR(QUOTE_DATE)<2016 AND BID_PRICE>0 LIMIT 1;


        IF bid_count != 0 THEN

            IF lastbid >0 THEN
                SET bid_change=ABS(thisbid-lastbid);
                SET alt_thisbid=alt_lastbid+(bid_change*direction*amp);
                SET alt_lastbid=alt_thisbid;
                SET lastbid=thisbid;

                /*select "bid change",bid_change;*/
            ELSE  /*first read*/
                SET lastbid=thisbid;
                SET alt_lastbid=thisbid;
                SET alt_thisbid=thisbid;

            END IF;


            UPDATE STOCK_QUOTE
            SET
                QUOTE_DATE=DATE_ADD(QUOTE_DATE, INTERVAL 11 YEAR),
                QUOTE_TIME=DATE_ADD(QUOTE_TIME, INTERVAL 11 YEAR),
                BID_PRICE = alt_thisbid
            WHERE   INSTRUMENT_ID = instr
                    AND QUOTE_SEQ_NBR = quoteseq
                    AND BID_PRICE > 0;

            COMMIT;
            SET bid_count = bid_count-1;

        END IF;



        SELECT ASK_PRICE, QUOTE_SEQ_NBR
        INTO thisask, quoteseq FROM STOCK_QUOTE
        WHERE   INSTRUMENT_ID = instr
        AND YEAR(QUOTE_DATE) < 2016
        AND ASK_PRICE > 0
        LIMIT 1;


        IF ask_count != 0 THEN

            IF lastask >0 THEN
                SET ask_change=ABS(thisask-lastask);
                SET alt_thisask=alt_lastask+(ask_change*direction*amp);
                SET alt_lastask=alt_thisask;
                SET lastask=thisask;
                /*select "ask change", ask_change;*/
            ELSE  /*first read*/
                SET lastask=thisask;
                SET alt_lastask=thisask;
                SET alt_thisask=thisask;

            END IF;



            UPDATE STOCK_QUOTE
            SET QUOTE_DATE = DATE_ADD(QUOTE_DATE, INTERVAL 11 YEAR),
                QUOTE_TIME = DATE_ADD(QUOTE_TIME, INTERVAL 11 YEAR),
                ASK_PRICE = alt_thisask
            WHERE INSTRUMENT_ID = instr
                    AND QUOTE_SEQ_NBR = quoteseq
                    AND ASK_PRICE > 0;

            COMMIT;

            SET ask_count = ask_count-1;

        END IF;

        SET loopcount=loopcount+1;
        SET rand_switchpoint=ROUND((RAND()+0.5)*switchpoint);

        IF loopcount%rand_switchpoint=0 THEN
          SET direction=direction*-1;
          -- SELECT 'direction changed', rand_switchpoint, direction, loopcount;

        END IF;

    END WHILE;

END
