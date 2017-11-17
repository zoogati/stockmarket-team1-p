DELIMITER $$
CREATE DEFINER=`socrates`@`%` PROCEDURE `makewaves`(IN tr_symbol varchar(15) , IN switchpoint int ,IN amp decimal(18,2))
BEGIN

/*declarations*/
	declare loopcount int;
	declare rand_switchpoint int;
	declare quotecount int;
	declare quoteseq int;
	declare direction tinyint;
	declare lastbid decimal(18,2);
	declare thisbid decimal(18,2);
	declare alt_thisbid decimal(18,2);
	declare alt_lastbid decimal(18,2);
	declare bid_change decimal(18,2);
	declare lastask decimal(18,2);
	declare thisask decimal(18,2);
	declare alt_thisask decimal(18,2);
	declare alt_lastask decimal(18,2);
	declare ask_change decimal(18,2);
	
	/*initialize*/
	set loopcount=0;
	set rand_switchpoint=0;
	set quotecount=0;
	set quoteseq=0;
	set direction=1;
	set lastbid=0;
	set thisbid=0;
	set alt_thisbid=0;
	set alt_lastbid=0;
	set bid_change=0;
	set lastask=0;
	set thisask=0;
	set alt_thisask=0;
	set alt_lastask=0;
	set ask_change=0;
	
	/* start bid loop*/
	SELECT 
    COUNT(*)
INTO quotecount FROM
    STOCK_QUOTE
WHERE
    TRADING_SYMBOL = tr_symbol;
SELECT 'quotes to process', quotecount;
	 while loopcount<quotecount do  /*main loop*/
		select BID_PRICE into thisbid from STOCK_QUOTE where TRADING_SYMBOL=tr_symbol and YEAR(QUOTE_TIME)<2016 and BID_PRICE>0 limit 1;
		SELECT 
    QUOTE_SEQ_NBR
INTO quoteseq FROM
    STOCK_QUOTE
WHERE
    TRADING_SYMBOL = tr_symbol
        AND YEAR(QUOTE_TIME) < 2016
        AND BID_PRICE > 0
LIMIT 1;
		
		if lastbid >0 then
			set bid_change=ABS(thisbid-lastbid);
			set alt_thisbid=alt_lastbid+(bid_change*direction*amp);
			set alt_lastbid=alt_thisbid;
			set lastbid=thisbid;
			/*select "bid change",bid_change;*/
		else  /*first read*/
        		set lastbid=thisbid;
			set alt_lastbid=thisbid;
			set alt_thisbid=thisbid;

		end if;
		UPDATE STOCK_QUOTE 
SET 
    QUOTE_DATE = NOW(),
    QUOTE_TIME = NOW(),
    BID_PRICE = alt_thisbid
WHERE
    TRADING_SYMBOL = tr_symbol
        AND QUOTE_SEQ_NBR = quoteseq
        AND BID_PRICE > 0;
		commit;
		
	      	 
		SELECT 
    ASK_PRICE
INTO thisask FROM
    STOCK_QUOTE
WHERE
    TRADING_SYMBOL = tr_symbol
        AND YEAR(QUOTE_TIME) < 2016
        AND ASK_PRICE > 0
LIMIT 1;
		SELECT 
    QUOTE_SEQ_NBR
INTO quoteseq FROM
    STOCK_QUOTE
WHERE
    TRADING_SYMBOL = tr_symbol
        AND YEAR(QUOTE_TIME) < 2016
        AND ASK_PRICE > 0
LIMIT 1;
		
		if lastask >0 then
			set ask_change=ABS(thisask-lastask);
			set alt_thisask=alt_lastask+(ask_change*direction*amp);
			set alt_lastask=alt_thisask;
			set lastask=thisask;
			/*select "ask change", ask_change;*/
		else  /*first read*/
        		set lastask=thisask;
			set alt_lastask=thisask;
			set alt_thisask=thisask;

		end if;
		UPDATE STOCK_QUOTE 
SET 
    QUOTE_DATE = now(),
    QUOTE_TIME = now(),
    ASK_PRICE = alt_thisask
WHERE
    TRADING_SYMBOL = tr_symbol
        AND QUOTE_SEQ_NBR = quoteseq
        AND ASK_PRICE > 0;
		commit;
		

                set loopcount=loopcount+1;
		set rand_switchpoint=ROUND((RAND()+0.5)*switchpoint);
		
		if loopcount%rand_switchpoint=0 then
		  set direction=direction*-1;
		SELECT 'direction changed', rand_switchpoint, direction, loopcount;
		end if;
		END while;
END$$
DELIMITER ;