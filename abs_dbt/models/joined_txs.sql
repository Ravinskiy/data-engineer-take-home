{{
    config(
        materialized='table',
        schema='public'
    )
}}

SELECT
    txs.block_timestamp,
    txs."from" AS from_address,
    txs."to" AS to_address,
    txs.transaction_hash,
    logs.topics
FROM {{ source('postgres', 'transactions') }} AS txs
JOIN {{ source('postgres', 'logs') }} AS logs
    ON logs.transaction_hash = txs.transaction_hash
ORDER BY txs.block_timestamp ASC