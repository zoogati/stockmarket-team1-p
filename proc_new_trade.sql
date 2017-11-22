CREATE DEFINER=`moustafa`@`%` PROCEDURE `proc_new_trade`(IN INSTR INT, IN SEQ INT, IN T_TIME DATETIME, IN SIZE INT, IN PRICE DECIMAL(18,4))
this_proc:BEGIN
DECLARE this_order_id, this_target_seq, this_order_size INT;
DECLARE this_order_price DECIMAL(18,4); DECLARE this_order_type VARCHAR(15);
DECLARE transaction_type VARCHAR(15); DECLARE order_count INT;
DECLARE this_profit, this_revenue, this_cost DECIMAL(18,4);


 IF SEQ NOT IN (SELECT TARGET_QUOTE FROM HFT_TRANSACTION WHERE TARGET_INSTR = INSTR AND TARGET_QUOTE = SEQ) THEN LEAVE this_proc; END IF;

 
 SELECT COUNT(*) INTO order_count FROM FLASH_ORDER WHERE INSTRUMENT_ID = INSTR
									 AND QUOTE_PRICE = PRICE AND QUOTE_SEQ_NBR = SEQ AND QUOTE_SIZE = SIZE
                                                    AND ORDER_STATUS = 'created';
 -- select 'count', order_count;

 IF order_count = 0 THEN LEAVE this_proc; END IF;

 SELECT ORDER_ID, TARGET_QUOTE, QUOTE_TYPE, QUOTE_SIZE, QUOTE_PRICE 
 INTO  this_order_id, this_target_seq, this_order_type, this_order_size, this_order_price
					   FROM FLASH_ORDER F WHERE F.INSTRUMENT_ID = INSTR 
										AND F.QUOTE_PRICE = PRICE	AND F.QUOTE_SEQ_NBR = SEQ AND F.QUOTE_SIZE = SIZE
                                                    AND F.ORDER_STATUS = 'created'
										ORDER BY F.QUOTE_TIME LIMIT 1;

SELECT ORDER_TYPE, COST, REVENUE, PROFIT INTO transaction_type, this_cost, this_revenue, this_profit
    FROM HFT_TRANSACTION WHERE TARGET_INSTR = INSTR AND TARGET_QUOTE = SEQ;
    
IF transaction_type = 'short' THEN
	IF this_order_type = 'ask' THEN
			SET this_cost = this_cost + this_order_size * this_order_price;
	ELSE
			SET this_revenue = this_revenue + this_order_size * this_order_price;
	END IF;
	SET this_profit = this_revenue - this_cost;
ELSE
    
    IF this_order_type = 'ask' THEN
			SET this_revenue = this_revenue + this_order_size * this_order_price;
	ELSE
			SET this_cost = this_cost + this_order_size * this_order_price;
	END IF;
    SET this_profit = this_revenue - this_cost;
END IF;

-- SELECT this_order_size, this_order_price, this_order_type, this_revenue, this_cost, this_profit;

UPDATE FLASH_ORDER SET ORDER_STATUS = 'processed' WHERE ORDER_ID = this_order_id AND QUOTE_SEQ_NBR = SEQ AND INSTRUMENT_ID = INSTR;
UPDATE HFT_TRANSACTION SET PROFIT = this_profit, COST = this_cost, REVENUE = this_revenue, NUM_TRADES=NUM_TRADES+1 WHERE TARGET_INSTR = INSTR AND TARGET_QUOTE = SEQ;    
  
END