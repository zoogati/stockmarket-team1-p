use stockmarket;
select 'Initializing Tables with 20 Instruments';
call initialize_tables(20);
select 'Makewaves on all of Stock_QUOTE';
select 'WARNING: WILL TAKE A LONG TIME';
call update_stock_quote();
