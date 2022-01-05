import pymysql.cursors
import requests 
from web3 import Web3
from datetime import datetime

conn = pymysql.connect(
    host="127.0.0.1",
    user="root",
    password="",
    database="flashbots",
    cursorclass=pymysql.cursors.DictCursor,
    port=3306
)

w3 = Web3(Web3.HTTPProvider('https://mainnet.infura.io/v3/63c6341ccc124ac39ce4472f3133154b'))
blocks_schema = ['block_number', 'miner', 'miner_reward', 'coinbase_transfers', 'gas_used', 'gas_price', 'timestamp']
transactions_schema = ['transaction_hash', 'tx_index', 'bundle_type', 'bundle_index', 'block_number', 'eoa_address', 'to_address', 'gas_used', 'gas_price', 'coinbase_transfer', 'total_miner_reward']

def turn_off_autocommit():
    with conn.cursor() as cur:
        cur.execute("SET autocommit = 0")
        cur.fetchone()
        conn.commit()

def get_latest_blocks():
    r = requests.get('https://blocks.flashbots.net/v1/blocks?limit=10000')
    j = r.json()
    return j['blocks'] # TODO: Use latest_block_number for early termination

def convert_row_to_insert_tuples(row, schema):
    inorder = []
    for col in schema:
        inorder.append(row[col])
    return "({})".format(','.join("'{}'".format(str(v)) for v in inorder))

def import_data(blocks):
    for block in blocks:
        transactions = block['transactions']
        del block['transactions']
        with conn.cursor() as cur:
            block['timestamp'] = datetime.fromtimestamp(w3.eth.get_block(block['block_number']).timestamp)
            cur.execute("INSERT IGNORE INTO blocks VALUES {}".format(convert_row_to_insert_tuples(block, blocks_schema)))
            cur.fetchall()
            for transaction in transactions:
                cur.execute("INSERT IGNORE INTO transactions VALUES {}".format(convert_row_to_insert_tuples(transaction, transactions_schema)))
                cur.fetchall()
                
blocks = get_latest_blocks()
import_data(blocks=blocks)

with conn.cursor() as cur:
    cur.execute("SELECT DOLT_COMMIT('-a', '-m', 'Added blocks')")
    cur.fetchall()
    cur.execute("SELECT DOLT_PUSH('origin', 'main')")
    cur.fetchall()
