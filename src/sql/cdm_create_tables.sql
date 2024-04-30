DROP TABLE IF EXISTS STV2024012236__DWH.global_metrics CASCADE;
CREATE TABLE STV2024012236__DWH.global_metrics
(
    date_update timestamp ,
    currency_from int NOT NULL,
    amount_total  numeric(17,2) NOT NULL,
    cnt_transactions int NOT NULL,
    avg_transactions_per_account numeric(17,2) NOT NULL,
	cnt_accounts_make_transactions int NOT NULL
);