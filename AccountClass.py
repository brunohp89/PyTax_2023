import os.path
import difflib
from tax_library import import_function_from_script
import inspect
import pandas as pd
from tax_library import balances, balances_fiat, income, generate_xlsx


class Account:
    def __init__(
        self, account_name, year=None, fiat_currency="eur", cummulative_results=True
    ):
        self.year = year
        self.fiat_currency = fiat_currency
        self.cummulative = cummulative_results
        self.script = self._get_script(account_name)
        self.transactions = pd.DataFrame()
        self.balances = None
        self.income = {
            "fiat": None,
            "crypto": None,
            "fiat_and_cashback": None,
            "crypto_and_cashback": None,
        }
        self.balances_fiat = None
        self.fiat_investment = 0
        self.get_transactions_args = self._get_transactions_args()

    def _get_script(self, account_name):
        libs = difflib.get_close_matches(
            account_name,
            [
                k1.replace(".py", "")
                for k1 in os.listdir(os.path.abspath("calculators"))
            ],
        )
        if len(libs) == 0:
            raise AttributeError(
                "No module with that name found, try to be more specific or correct your spelling"
            )
        script = str(libs[0])
        print(
            f"{script.title()} identified, if this is not correct recreate the object with a more specific account name"
        )
        return script

    def _get_transactions_args(self):
        func = import_function_from_script(self.script, "get_transactions_df")
        args = inspect.signature(func).parameters.keys()
        req_args = [arg for arg in args if "=" not in arg]
        opt_args = [arg.split("=")[0] for arg in args if "=" in arg]
        if len(req_args) > 0:
            print(
                f'For this account the get_transactions function takes the following required arguments: {", ".join(req_args)}'
            )
        if len(opt_args) > 0:
            print(
                f'For this account the get_transactions function takes the following optional arguments: {", ".join(opt_args)}'
            )
        return req_args, opt_args

    def get_transactions_func(self, **kwargs):
        func = import_function_from_script(self.script, "get_transactions_df")
        return func(**kwargs)

    def get_transactions(self, **kwargs):
        self.transactions = self.get_transactions_func(**kwargs)

    def get_balances(self):
        if self.transactions.shape[0] == 0:
            raise ValueError(
                "Transaction dataframe is empty, call get_transactions method first with the required arguments"
            )
        self.balances = balances(
            self.transactions, cummulative=self.cummulative, year_sel=self.year
        )
        self.balances_fiat = balances_fiat(
            self.balances, currency=self.fiat_currency, year_sel=self.year
        )

    def get_income(self):
        if self.transactions.shape[0] == 0:
            raise ValueError(
                "Transaction dataframe is empty, call get_transactions method first with the required arguments"
            )
        if "Cashback" in self.transactions["Notes"].tolist():
            self.income["fiat_and_cashback"] = income(
                self.transactions, type_out="fiat", year_sel=self.year, name=self.script
            )
            self.income["crypto_and_cashback"] = income(
                self.transactions,
                type_out="crypto",
                year_sel=self.year,
                name=self.script,
            )
        self.income["fiat"] = income(
            self.transactions,
            type_out="fiat",
            year_sel=self.year,
            name=self.script,
            include_cashback=False,
        )
        self.income["crypto"] = income(
            self.transactions,
            type_out="crypto",
            year_sel=self.year,
            name=self.script,
            include_cashback=False,
        )

    def get_investment(self, ret=True):
        try:
            func = import_function_from_script(self.script, "get_eur_invested")
            self.fiat_investment = func(year=self.year)
            if ret:
                return func(year=self.year)
        except BaseException as e:
            return None

    def generate_all(self, excel_output=True):
        self.get_investment(ret=False)
        data_xlsx = []
        sheet_names = [
            "Transactions",
            "Balances",
            f"Balances {self.fiat_currency}",
            "Income Crypto",
            f"Income {self.fiat_currency}",
            f"Income Crypto and Cashback",
            f"Income {self.fiat_currency} and Chashback",
        ]
        if self.transactions.shape[0] == 0:
            raise ValueError(
                "Transaction dataframe is empty, call get_transactions method first with the required arguments"
            )
        data_xlsx.append(self.transactions)
        if self.balances is None:
            self.get_balances()
        data_xlsx.append(self.balances)
        data_xlsx.append(self.balances_fiat)

        if self.income["crypto"] is None:
            self.get_income()
        data_xlsx.append(self.income["crypto"])
        data_xlsx.append(self.income["fiat"])
        data_xlsx.append(self.income["crypto_and_cashback"])
        data_xlsx.append(self.income["fiat_and_cashback"])
        if excel_output:
            generate_xlsx(f"{self.script} {self.year}.xlsx", sheet_names, data_xlsx)

    def set_year(self, year_in):
        self.year = year_in
