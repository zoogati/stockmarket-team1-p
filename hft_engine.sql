CREATE DEFINER=`moustafa`@`%` PROCEDURE `hft_engine`(IN instr INT, IN seq_nbr INT, IN q_time DATETIME, IN ord_type VARCHAR(10),
													IN price INT, IN volume INT, OUT num_quotes_injected INT)
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

  DECLARE new_instrument int(11);
  DECLARE new_quote_date date;
  DECLARE new_quote_seq_nbr int(11);
  DECLARE new_trading_symbol varchar(15);
  DECLARE new_quote_time datetime;
  DECLARE new_ask_price decimal(18,4);
  DECLARE new_ask_size int(11);
  DECLARE new_bid_price decimal(18,4);
  DECLARE new_bid_size int(11);

  declare trade_size int(11) DEFAULT 0;
  declare trade_price decimal(18,4);
  declare carry_over int(11);
  DECLARE num_quotes_injected INT(11) DEFAULT 0;
  DECLARE quote_count INT;
  DECLARE loop_count INT;
  DECLARE this_quote_size INT; DECLARE this_quote_price INT;
  DECLARE order_id INT;
  DECLARE best_price INT;


  -- DECLARE getQuote CURSOR FOR (SELECT QUOTE_PRICE, QUOTE_SIZE FROM FLASH_ORDER
  --                              WHERE INSTRUMENT_ID = new_instrument AND QUOTE_SEQ_NBR = new_quote_seq_nbr
  --                               AND ORDER_STATUS = 'pending');

  DECLARE cur1 CURSOR FOR SELECT * FROM STOCK_QUOTE_FEED
                                    WHERE INSTRUMENT_ID = instr_id
                                    AND QUOTE_SEQ_NBR < quote_sq_nb AND QUOTE_TIME <= new_quote_time
                                    AND (
                            (new_bid_price * ASK_PRICE > 0 AND ASK_PRICE <= new_bid_price+0.0002) OR
                                          (new_ask_price > 0 AND BID_PRICE >= new_ask_price+0.0002)
                                        )
                                      -- AND QUOTE_TIME > DATE_SUB(NOW(), INTERVAL 10 MIN)
                                   ORDER BY ASK_PRICE ASC, BID_PRICE DESC, QUOTE_SEQ_NBR, QUOTE_TIME;


  SET new_instrument = instr; SET new_quote_seq_nbr = seq_nbr; SET new_quote_time = q_time;

  IF ord_type = 'ask' THEN
    SET new_ask_price = price;
    SET new_ask_size = volume;
  ELSE
    SET new_bid_price = price;
    SET new_bid_size = volume;
  END IF;

  IF new_ask_price > 0 THEN
    SET trade_price =new_ask_price; SET trade_size = new_ask_size;
  ELSE
    SET trade_price =new_bid_price; SET trade_size = new_bid_size;
  END IF;

  SET carry_over = trade_size;


--  FIRST MAKE COUNTER OFFERS TO POTENTIAL MATCHES IN THE FEED
--  BY INSERTING OWN QUOTES AT V = SIZE OF INCOMING ORDER
--  THEN PRICE-MATCH EACH QUOTE IN THE FEED TO ACHEIVE MAXIMUM PROFIT

  OPEN cur1;
  BEGIN
    DECLARE exit_flag INT DEFAULT 0;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET exit_flag=1;

    scan_loop: LOOP

        FETCH cur1 INTO this_instrument,
                        this_quote_date,
                        this_quote_seq_nbr,
                        this_trading_symbol,
                        this_quote_time,
                        this_ask_price,
                        this_ask_size,
                        this_bid_price,
                        this_bid_size;

        IF (exit_flag OR carry_over = 0)
          THEN leave scan_loop;
        END IF;


        IF new_ask_price > 0 THEN -- ask quote- FIND HIGHEST MATCHING BIDS --

                IF carry_over >= this_bid_size THEN
                  SET carry_over = carry_over - this_bid_size;

                  -- TRACK ORDER STATUS IN FLASH_ORDER TABLE
                  INSERT INTO FLASH_ORDER VALUES(
                    this_instrument,
                    this_trading_symbol,
                    seq_nbr,
                    q_time,
                    DATE(q_time),
                    'ask',
                    this_bid_price,
                    this_bid_size,
                    'pending');

                ELSE
                  -- # LEFTOVER QUOTES < VOLUME OF MATCHED QUOTE
                  INSERT INTO FLASH_ORDER VALUES(
                    this_instrument,
                    this_trading_symbol,
                    seq_nbr,
                    q_time,
                    DATE(q_time),
                    'ask',
                    this_bid_price,
                    carry_over,
                    'pending');
                    SET this_bid_size = this_bid_size - carry_over;
                    SET carry_over = 0;

                END IF;


        ELSE   -- BID QUOTE ->  FIND MATCHING ASKS -> INSERT OWN BIDS

                IF carry_over >= this_ask_size THEN

                    SET carry_over = carry_over - this_ask_size;
                    -- KEEP TRACK OF YOUR INSERTED QUOTES IN FLASH_ORDER TABLE
                    INSERT INTO FLASH_ORDER VALUES(
                      this_instrument,
                      this_trading_symbol,
                      seq_nbr,
                      q_time,
                      DATE(q_time),
                      'bid',
                      this_ask_price,
                      this_ask_size,
                      'pending');


                ELSE
                    -- # LEFTOVER QUOTES < VOLUME OF MATCHED QUOTE

                    -- KEEP TRACK OF YOUR INSERTED QUOTES IN FLASH_ORDER TABLE
                    INSERT INTO FLASH_ORDER VALUES(
                      this_instrument,
                      this_trading_symbol,
                      seq_nbr,
                      q_time,
                      DATE(q_time),
                      'bid',
                      this_ask_price,
                      carry_over,
                      'pending');
                    SET this_ask_size = this_ask_size - carry_over;
                    SET carry_over = 0;

                END IF;
        END IF;

    END LOOP;
    COMMIT;


   IF ( carry_over != 0 or (carry_over=0 AND exit_flag=1) ) THEN
                          --  VOLUME OF INCOMING QUOTE > TOTAL NUM OF MATCHES IN THE POOL -- TO ACHIEVE MAX PROFT
                          --  INSERT ALL QUOTES IN FLASH_ORDER AT PRICE (IN) TO MATCH QUOTES IN THE POOL
                          --  MAKE A *SINGLE QUOTE* -> VOLUME = TRADE_SIZE - CARRY_OVER

       IF ord_type = 'ask' THEN
            SELECT COUNT(*) INTO quote_count
            FROM FLASH_ORDER
            WHERE INSTRUMENT_ID = instr AND QUOTE_SEQ_NBR = seq_nbr
              AND QUOTE_TYPE = 'ask' AND ORDER_STATUS = 'pending';
            SET loop_count = 0;

            match_loop: LOOP

                  IF (loop_count = quote_count) THEN leave match_loop; END IF;

                  SELECT QUOTE_SIZE, QUOTE_PRICE, ORDER_ID
                  INTO this_quote_size, this_quote_price, order_id
                  FROM FLASH_ORDER
                  WHERE INSTRUMENT_ID = instr AND QUOTE_SEQ_NBR = seq_nbr
                  AND QUOTE_TYPE = 'ask' AND ORDER_STATUS = 'pending'
                  ORDER BY ORDER_ID LIMIT 1;

                  INSERT INTO STOCK_QUOTE_FEED VALUES(
                    this_instrument,
                    DATE(q_time),
                    seq_nbr,
                    this_trading_symbol,
                    DATE_ADD( q_time, INTERVAL num_quotes_injected SECOND),
                    this_quote_price,
                    this_quote_size,
                    0,0);

                  UPDATE FLASH_ORDER SET ORDER_STATUS = 'created' WHERE ORDER_ID = order_id;
                  SET loop_count = loop_count +1;
                  SET num_quotes_injected = num_quotes_injected +1;

            END LOOP;

            INSERT INTO STOCK_QUOTE_FEED VALUES(
              this_instrument,
              DATE(q_time),
              seq_nbr,
              this_trading_symbol,
              DATE_ADD( q_time, INTERVAL num_quotes_injected SECOND),
              0,0,
              new_ask_price + 0.0002,
              volume-carry_over);

            INSERT INTO FLASH_ORDER VALUES(
              this_instrument,
              this_trading_symbol,
              seq_nbr,
              q_time,
              DATE(q_time),
              'bid',
              new_ask_price + 0.0002,
              volume-carry_over,
              'created');

            SET num_quotes_injected = num_quotes_injected+1;

       ELSE

              SELECT COUNT(*) INTO quote_count
              FROM FLASH_ORDER
              WHERE INSTRUMENT_ID = instr AND QUOTE_SEQ_NBR = seq_nbr
                AND QUOTE_TYPE = 'bid' AND ORDER_STATUS = 'pending';
              SET loop_count = 0;

              match_loop: LOOP

                  IF (loop_count = quote_count) THEN leave match_loop; END IF;

                  SELECT QUOTE_SIZE, QUOTE_PRICE, ORDER_ID
                  INTO this_quote_size, this_quote_price, order_id
                  FROM FLASH_ORDER
                  WHERE INSTRUMENT_ID = instr AND QUOTE_SEQ_NBR = seq_nbr
                  AND QUOTE_TYPE = 'bid' AND ORDER_STATUS = 'pending'
                  ORDER BY ORDER_ID LIMIT 1;

                  INSERT INTO STOCK_QUOTE_FEED VALUES(
                    this_instrument,
                    DATE(q_time),
                    seq_nbr,
                    this_trading_symbol,
                    DATE_ADD( q_time, INTERVAL num_quotes_injected SECOND),
                    0,0,
                    this_quote_price,
                    this_quote_size);

                  UPDATE FLASH_ORDER SET ORDER_STATUS = 'created' WHERE ORDER_ID = order_id;
                  SET loop_count = loop_count +1;
                  SET num_quotes_injected = num_quotes_injected +1;

              END LOOP;


              INSERT INTO STOCK_QUOTE_FEED VALUES(
                this_instrument,
                DATE(q_time),
                seq_nbr,
                this_trading_symbol,
                DATE_ADD( q_time, INTERVAL num_quotes_injected SECOND),
                new_bid_price-0.0002,
                volume-carry_over,
                0,0);

              INSERT INTO FLASH_ORDER VALUES(
                this_instrument,
                this_trading_symbol,
                seq_nbr,
                q_time,
                DATE(q_time),
                'ask',
                new_bid_price - 0.0002,
                volume-carry_over,
                'created');

                SET num_quotes_injected = num_quotes_injected+1;

       END IF;


   ELSE -- VOL(IN) < #QUOTES IN THE POOL -> FIND COMPETING QUOTES THEN OUTBID/UNDERSELL BY 0.0001
        -- START AT THE LAST QUOTE POINTED TO BY THE CURSOR, MATCH REMINAING #QUOTES THEN MOVE ON TO SUBSEQUENT QUOTES
        -- TOTAL #QUOTES = VOLUME(IN) = TRADE_SIZE



        IF ord_type = 'ask' THEN

             IF this_bid_size = 0 THEN SET best_price = this_bid_price;
             ELSE SET best_price= this_bid_price +0.0001;

              DELETE FROM FLASH_ORDER WHERE ORDER_ID = (SELECT ORDER_ID FROM FLASH_ORDER
                                                        WHERE QUOTE_TYPE = 'ask' AND ORDER_STATUS ='pending'
                                                        AND INSTRUMENT_ID = inst AND QUOTE_SEQ_NBR = seq_nbr
                                                        ORDER BY ORDER_ID DESC LIMIT 1);
             END IF;

             COMMIT;

             SELECT COUNT(*) INTO quote_count
             FROM FLASH_ORDER
             WHERE INSTRUMENT_ID = instr AND QUOTE_SEQ_NBR = seq_nbr
               AND QUOTE_TYPE = 'ask' AND QUOTE_PRICE > best_price
               AND ORDER_STATUS = 'pending';

             SET loop_count = 0;
             SET trade_size = 0;

             match_loop: LOOP

                   IF (loop_count = quote_count) THEN leave match_loop; END IF;

                   SELECT QUOTE_SIZE, QUOTE_PRICE, ORDER_ID
                   INTO this_quote_size, this_quote_price, order_id
                   FROM FLASH_ORDER
                   WHERE INSTRUMENT_ID = instr AND QUOTE_SEQ_NBR = seq_nbr
                   AND QUOTE_TYPE = 'ask' AND ORDER_STATUS = 'pending'
                   ORDER BY ORDER_ID LIMIT 1;

                   INSERT INTO STOCK_QUOTE_FEED VALUES(
                     this_instrument,
                     DATE(q_time),
                     seq_nbr,
                     this_trading_symbol,
                     DATE_ADD( q_time, INTERVAL num_quotes_injected SECOND),
                     this_quote_price,
                     this_quote_size,
                     0,0);

                   UPDATE FLASH_ORDER SET ORDER_STATUS = 'created' WHERE ORDER_ID = order_id;
                   SET loop_count = loop_count +1;
                   SET num_quotes_injected = num_quotes_injected +1;
                   SET trade_size = trade_size + this_quote_size;

             END LOOP;

             INSERT INTO STOCK_QUOTE_FEED VALUES(
               this_instrument,
               DATE(q_time),
               seq_nbr,
               this_trading_symbol,
               DATE_ADD( q_time, INTERVAL num_quotes_injected SECOND),
               0,0,
               best_price,
               trade_size);

             DELETE FROM FLASH_ORDER WHERE INSTRUMENT_ID = instr AND
              QUOTE_SEQ_NBR = seq_nbr AND ORDER_STATUS = 'pending';

             INSERT INTO FLASH_ORDER VALUES(
               this_instrument,
               this_trading_symbol,
               seq_nbr,
               q_time,
               DATE(q_time),
               'bid',
               best_price,
               trade_size,
               'created');


             SET num_quotes_injected = num_quotes_injected+1;

        ELSE

              IF this_ask_size = 0 THEN SET best_price = this_ask_price;
              ELSE SET best_price= this_bid_price -0.0001;

               DELETE FROM FLASH_ORDER WHERE ORDER_ID = (SELECT ORDER_ID FROM FLASH_ORDER
                                                         WHERE QUOTE_TYPE = 'bid' AND ORDER_STATUS ='pending'
                                                         AND INSTRUMENT_ID = inst AND QUOTE_SEQ_NBR = seq_nbr
                                                         ORDER BY ORDER_ID DESC LIMIT 1);
              END IF;

              COMMIT;

              SELECT COUNT(*) INTO quote_count
              FROM FLASH_ORDER
              WHERE INSTRUMENT_ID = instr AND QUOTE_SEQ_NBR = seq_nbr
                AND QUOTE_TYPE = 'bid' AND QUOTE_PRICE < best_price
                AND ORDER_STATUS = 'pending';

              SET loop_count = 0;
              SET trade_size = 0;

              match_loop: LOOP

                    IF (loop_count = quote_count) THEN leave match_loop; END IF;

                    SELECT QUOTE_SIZE, QUOTE_PRICE, ORDER_ID
                    INTO this_quote_size, this_quote_price, order_id
                    FROM FLASH_ORDER
                    WHERE INSTRUMENT_ID = instr AND QUOTE_SEQ_NBR = seq_nbr
                    AND QUOTE_TYPE = 'bid' AND ORDER_STATUS = 'pending'
                    ORDER BY ORDER_ID LIMIT 1;

                    INSERT INTO STOCK_QUOTE_FEED VALUES(
                      this_instrument,
                      DATE(q_time),
                      seq_nbr,
                      this_trading_symbol,
                      DATE_ADD( q_time, INTERVAL num_quotes_injected SECOND),
                      0,0,
                      this_quote_price,
                      this_quote_size);

                    UPDATE FLASH_ORDER SET ORDER_STATUS = 'created' WHERE ORDER_ID = order_id;
                    SET loop_count = loop_count +1;
                    SET num_quotes_injected = num_quotes_injected +1;
                    SET trade_size = trade_size + this_quote_size;

              END LOOP;

              INSERT INTO STOCK_QUOTE_FEED VALUES(
                this_instrument,
                DATE(q_time),
                seq_nbr,
                this_trading_symbol,
                DATE_ADD( q_time, INTERVAL num_quotes_injected SECOND),
                best_price,
                trade_size,
                0,0);

              DELETE FROM FLASH_ORDER WHERE INSTRUMENT_ID = instr AND
               QUOTE_SEQ_NBR = seq_nbr AND ORDER_STATUS = 'pending';

              INSERT INTO FLASH_ORDER VALUES(
                this_instrument,
                this_trading_symbol,
                seq_nbr,
                q_time,
                DATE(q_time),
                'ask',
                best_price,
                trade_size,
                'created');


              SET num_quotes_injected = num_quotes_injected+1;
        END IF;

   END IF;

  END;
  CLOSE cur1;

END
