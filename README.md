#PyTax 2023
PyTax is a repository containing a series of python scripts that allow you to download and format all your crypto transactions. Each script is designed to track transactions made on a specific CEX (Centralized Exchange) or blockchain. The ultimate goal of this project is to correctly calculate taxes to be paid.

##Features
- The CEXs currently supported are: Binance, Crypto.com, Crypto.com Exchange, Uphold, Coinbase, and Kucoin.
- The blockchains supported are Ethereum, Bitcoin, Solana, Cardano, BSC, Cronos Chain, NEAR, and Polygon.
- For Ethereum and Polygon, there's a function to track all your NFT buys and sells.
- The function ```get_transactions_df``` in each script requires either a wallet address (for blockchains) or a folder containing all .csv files downloaded from the CEX.
- The output of each script is a properly formatted dataframe that can be concatenated with other dataframes produced by different scripts.
- The repository includes a requirements.txt file for easy installation of required packages.
- API tokens for Ethereum (etherscan.io), BSC (bscscan.com), Polygon (polygonscan.com), and Cardano (blockfrost.io) are required and should be stored in a .json file in the same directory as the scripts.
- Importing the Scripts
- The scripts in PyTax can be imported like any other python package, allowing you to call the functions inside the script in your own projects.

##Usage
To use the scripts in PyTax, simply follow these steps:

1. Clone or download the repository.
2. Install the required packages listed in the requirements.txt file.
3. Store your API tokens in a .json file in the same directory as the scripts (see example .json file).
4. Import the script for the CEX or blockchain you want to track transactions for.
5. Call the function ```get_transactions_df``` and pass in either a wallet address or folder containing .csv files as arguments.

##Example
Here is an example of how you could use the script for Binance:

``` python
import binance

df = binance.get_transactions_df('path/to/csv_folder')
``` 

And for Ethereum:

``` python
import ethereum

df = ethereum.get_transactions_df(wallet_address, ethscan_token)
``` 

##Tax Library
The tax_library script that is a part of the PyTax repository contains a few useful functions to extract information from the transactions dataframe generated by the ```get_transactions_df``` function.

###Functions
```income```: The income function calculates the daily amount of interest or rewards earned based on a given transactions dataframe. By default, the income is calculated in fiat value, but it can also be calculated in the native asset by passing ```type_out='crypto'```. The function provides several options to customize the calculation, including the ability to filter by year, include or exclude cashbacks (i.e. Binance Card cashbacks), and allow negative income.
```python
import tax_library as tx

tx.income(
    transactions: pd.DataFrame,
    type_out="fiat",
    cummulative=True,
    year_sel=None,
    name=None,
    include_cashback=True,
    allow_negative=False,
)

```

```balances```: The balances function provides the capability to calculate the daily balances of each asset from a specified transactions dataframe. The calculation can be performed cumulatively or not, and it can be further refined by selecting a specific year for the calculation.
``` python
import tax_library as tx

tx.balances(
    transactions: pd.DataFrame,
    cummulative=True,
    year_sel=None
)
```

```balances_fiat```: The balances_fiat function calculates the fiat value of the balances dataframe, which is generated by the ```balances``` function. The calculation utilizes the prices parameter, which is an object of the ```PricesClass```.

``` python
import tax_library as tx

balances_fiat(
    balances: pd.DataFrame,
    prices: Prices,
    currency="eur",
    year_sel=None
)
```
Note: This script will later be broken down into two parts as it also contains some utility functions used in other scripts in the PyTax scripts.

##Contributing
If you want to contribute to this project, feel free to create a pull request with any updates or bug fixes. If you have any questions or issues with the scripts, please open an issue in the repository.

##Conclusion
Whether you're an experienced cryptocurrency trader or just starting out, these scripts can help you keep track of all your transactions in one convenient place. By using these scripts, you can save time and ensure that all of your transaction data is accurate and up-to-date. Thanks for using PyTax!