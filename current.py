import json
from collections import defaultdict
import psycopg

conn = psycopg.connect(
    dbname='postgres',
    host='localhost',
    user='postgres',
    password='password',
    port='5432'
)

wallets = defaultdict(list)
wallets_first_ts = dict()
wallets_last_ts = dict()

cursor = conn.cursor()

cursor.execute('''
    SELECT txs.block_timestamp, "from", "to", txs.transaction_hash, logs.topics
    FROM transactions AS txs
    JOIN logs ON logs.transaction_hash = txs.transaction_hash
    ORDER BY txs.block_timestamp ASC
''')

counter = 0
for record in cursor:
    tx_timestamp = record[0]
    from_addr = record[1]
    to_addr = record[2]
    topics = record[4]
    for topic in topics:
        try:
            wallets[from_addr].remove(topic)
        except ValueError:
            ...
    if to_addr:
        for topic in topics:
            wallets[to_addr].append(topic)
    if from_addr and from_addr not in wallets_first_ts:
        wallets_first_ts[from_addr] = tx_timestamp
    if to_addr and to_addr not in wallets_first_ts:
        wallets_first_ts[to_addr] = tx_timestamp
    wallets_last_ts[from_addr] = tx_timestamp
    wallets_last_ts[to_addr] = tx_timestamp

    counter += 1
    if counter >= 20:
        break

pretty_wallsets = json.dumps(wallets, indent=2)
print(pretty_wallsets)
print()
conn.close()
