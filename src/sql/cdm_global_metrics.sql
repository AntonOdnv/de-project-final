MERGE INTO STV2024012236__DWH.global_metrics AS gm
USING (
WITH all_transactions AS (
        SELECT operation_id,
               currency_code as currency_from,
               account_number_from,
               transaction_dt,
               amount,
               status
        FROM STV2024012236__STAGING.transactions
        WHERE currency_code=420
          AND status='done'
          AND account_number_from>0
          AND transaction_dt::date='{dt}'::date
          AND transaction_type !='authorization'
        
        UNION ALL
          
        SELECT t.operation_id AS operation_id,
               t.currency_code AS currency_from,
               t.account_number_from AS account_number_from,
               t.transaction_dt AS transaction_dt,
               (t.amount*c.currency_with_div) AS amount,
               t.status AS status
        FROM STV2024012236__STAGING.transactions AS t
        LEFT JOIN STV2024012236__STAGING.currencies AS c ON t.currency_code=c.currency_code  and c.date_update::date=t.transaction_dt::date
        WHERE t.currency_code!=420 
          AND c.currency_code_with=420
          AND t.status='done'
          AND t.account_number_from>0
          AND t.transaction_dt::date='{dt}'::date
          AND t.transaction_type !='authorization'
        )

SELECT transaction_dt AS date_update,
       currency_from,
       sum(amount) as amount_total, 
       count(*) as cnt_transactions,
       round(sum(amount)/count(distinct account_number_from),2) as avg_transactions_per_account,
       count(distinct account_number_from) as cnt_accounts_make_transactions
FROM all_transactions
GROUP BY  date_update, currency_from ) AS cdm_data

ON cdm_data.date_update=gm.date_update AND cdm_data.currency_from=gm.currency_from

WHEN MATCHED THEN UPDATE SET
                currency_from = cdm_data.currency_from,
                amount_total = cdm_data.amount_total, 
                cnt_transactions = cdm_data.cnt_transactions,  
                avg_transactions_per_account = cdm_data.avg_transactions_per_account,
                cnt_accounts_make_transactions = cdm_data.cnt_accounts_make_transactions

WHEN NOT MATCHED
    THEN INSERT (
                date_update,
                currency_from,
                amount_total, 
                cnt_transactions,  
                avg_transactions_per_account,
    			cnt_accounts_make_transactions
        )
    VALUES (
                cdm_data.date_update,
                cdm_data.currency_from,
                cdm_data.amount_total, 
                cdm_data.cnt_transactions,  
                cdm_data.avg_transactions_per_account,
    cdm_data.cnt_accounts_make_transactions
        );
        