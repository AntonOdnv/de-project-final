DROP TABLE IF EXISTS STV2024012236__STAGING.transactions CASCADE;
DROP PROJECTION IF EXISTS STV2024012236__STAGING.transactions_b0 CASCADE;
CREATE TABLE STV2024012236__STAGING.transactions
(
    operation_id uuid NOT NULL,
    account_number_from int NOT NULL,
    account_number_to int NOT NULL,
    currency_code int NOT NULL,
    country varchar NOT NULL,
    status varchar NOT NULL CHECK (status IN ('queued', 'in_progress', 'blocked', 'done', 'chargeback')),
    transaction_type varchar NOT NULL,
    amount int NOT NULL, 
    transaction_dt timestamp, 
    CONSTRAINT C_PRIMARY PRIMARY KEY (operation_id, status, transaction_dt) DISABLED
) 
ORDER BY operation_id
SEGMENTED BY HASH(operation_id,transaction_dt) ALL NODES
PARTITION BY transaction_dt::DATE;
 
    
CREATE PROJECTION STV2024012236__STAGING.transactions_b0
(
	operation_id,
	account_number_from,
	account_number_to,
	currency_code,
	country,
	status,
	transaction_type,
	amount,
	transaction_dt 
)
AS
SELECT transactions.operation_id,
 	   transactions.account_number_from,
       transactions.account_number_to,
       transactions.currency_code,
       transactions.country,
       transactions.status,
       transactions.transaction_type,
       transactions.amount,
       transactions.transaction_dt
FROM STV2024012236__STAGING.transactions
ORDER BY transactions.transaction_dt
SEGMENTED BY hash( transactions.transaction_dt) ALL NODES KSAFE 1;

DROP TABLE IF EXISTS STV2024012236__STAGING.currencies CASCADE;
DROP PROJECTION IF EXISTS STV2024012236__STAGING.currencies_b0 CASCADE;
CREATE TABLE STV2024012236__STAGING.currencies
(
    currency_code int NOT NULL,
    currency_code_with int NOT NULL,
    date_update timestamp NOT NULL,
    currency_with_div numeric(5,2) NOT NULL,
    CONSTRAINT C_PRIMARY PRIMARY KEY (date_update, currency_code, currency_code_with) ENABLED 
)
ORDER BY currency_code, currency_code_with
SEGMENTED BY HASH(currency_code, currency_code_with, date_update) ALL NODES
PARTITION BY date_update::DATE;


CREATE PROJECTION STV2024012236__STAGING.currencies_b0
(
	currency_code,
	currency_code_with,
	date_update,
	currency_with_div 
)
AS
SELECT currencies.currency_code,
       currencies.currency_code_with,
       currencies.date_update,
       currencies.currency_with_div 
FROM STV2024012236__STAGING.currencies
ORDER BY currencies.date_update 
SEGMENTED BY hash(currencies.date_update) ALL NODES KSAFE 1;
