DELIMITER $$
CREATE DEFINER=`socrates`@`%` PROCEDURE `update_stock_quote`()
BEGIN

declare this_trading_symbol varchar(15);
declare this_switchpoint int(11);
declare this_amplitude decimal(18,4);
declare loopcount int(11);
declare cur1 cursor for (select Trading_Symbol, SWITCHPOINT, amplitude from QUOTE_ADJUST as qa join INSTRUMENT as inst where qa.INSTRUMENT_ID = inst.INSTRUMENT_ID);

set loopcount = (select count(*) from QUOTE_ADJUST);
open cur1;

while loopcount > 0 do
	fetch cur1 into this_trading_symbol, this_switchpoint, this_amplitude;
	set loopcount = loopcount - 1;
	call makewaves(this_trading_symbol, this_switchpoint, this_amplitude);
end while;

close cur1;
END$$
DELIMITER ;