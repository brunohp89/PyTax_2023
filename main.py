import binance
import coinbase as cb
import cryptodotcom as cdc
import cryptodotcom_exchange as cdce
import uphold as up
import binance as bina
import tax_library as tx

binance_df = bina.get_transactions_df()
coin_df = cb.get_transactions_df()
cdc_df = cdc.get_transactions_df()
cdce_df = cdce.get_transactions_df()
up_df = up.get_transactions_df()

tx.calcolo_giacenza_media(tx.balances(up_df))

dd=tx.income(cdc_df, year_sel=2022, include_cashback=True)


