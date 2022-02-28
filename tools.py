from terra_sdk.client.lcd import LCDClient
import terra_sdk
import pandas as pd
import numpy as np
import re
import requests


terra = LCDClient(chain_id="columbus-5", url="https://lcd.terra.dev")


def wallet_balance(wallet):
    """
    This Function takes in an input of a wallet address 
    and returns a DataFrame that will include the Coin units and the Balance of each unit
    """
    if terra_sdk.core.bech32.is_acc_address(wallet) == False:
        return "Your entry must be a bech32 Terra address with a dtype str"
    else:
        wallet_balance = terra.bank.balance(wallet)
        wallet_data = wallet_balance[0].to_data()
        df = pd.DataFrame(wallet_data)
        df['amount'] = df['amount'].astype(int)/1000000
        return df

def return_transaction_data(wallet):
    """
    This function takes an input of a wallet address and returns transaction information

    """
    if terra_sdk.core.bech32.is_acc_address(wallet) == False:
        return "Your entry must be a bech32 Terra address with a dtype str"
    else:
        wallet_transactions = requests.get(f'https://fcd.terra.dev/v1/txs?account={wallet}&chainId=columbus-5&limit=100').json()

        transaction_df = pd.DataFrame(wallet_transactions['txs'])
        transaction_df['timestamp'] = pd.to_datetime(transaction_df['timestamp'])
        transaction_df = transaction_df.set_index(transaction_df['timestamp'])

        columns_to_drop = ['timestamp', 'raw_log']
        transaction_df = transaction_df.drop(columns = columns_to_drop)
        return transaction_df

def get_validator_df():
    """
    Function takes in no input but returns the staking and validator data for all active
    validators for the terra ecosystem
    """

    validator_data = requests.get('https://fcd.terra.dev/v1/staking').json()

    validator_df = pd.DataFrame(validator_data['validators'])

    validator_df['description'] = validator_df['description'].apply(lambda x: x.get('moniker'))
    validator_df['votingPower'] = validator_df['votingPower'].apply(lambda x: x.get('weight'))
    validator_df['commissionInfo'] = validator_df['commissionInfo'].apply(lambda x: x.get('rate'))
    validator_df = validator_df.loc[validator_df['status'] == 'active']
    validator_df = validator_df.set_index('description')
    validator_df['delegatorShares'] = validator_df['delegatorShares'].astype(float)/1000000
    validator_df['tokens'] = validator_df['tokens'].astype(float)/1000000
    validator_df['goodKarma'] = abs(validator_df['tokens'] - validator_df['delegatorShares'])
    validator_df = validator_df.drop(columns= ['rewardsPool', 'selfDelegation'])
    validator_df = validator_df.sort_values(by= 'votingPower', ascending=False)


    return validator_df

def total_token_supply():
    """
    This function takes no input but creates a df that shows the total token supply 
    """
    tot_supply = terra.bank.total()
    supply_data = tot_supply[0].to_data()
    df = pd.DataFrame(supply_data)
    df['amount'] = df['amount'].astype(int)/1000000

    return df

def create_stablecoin_df(stablecoin_index):
    """
    Takes an input of an index from the history of transaction growth 
    and creates a new dataframe that can be plotted to show growth of each stablecoin
    ---
    The Stablecoin exchange rates are called to normalize the transaction volume to be priced in Luna
    """
    transaction_growth = requests.get('https://fcd.terra.dev/v1/dashboard/tx_volume').json()

    # Gets historical data for txVolume
    transaction_df = pd.DataFrame(transaction_growth['cumulative']).sort_values('denom', ascending=False).reset_index()
    
    # Gets current exchange rates and then changes the index of the table to match the transaction table
    stablecoin_exchange_rates = pd.DataFrame(terra.oracle.exchange_rates().to_data())
    uluna_exchange_rate = pd.DataFrame([['uluna', 1]], columns = ['denom', 'amount'])
    stablecoin_exchange_rates = pd.concat([stablecoin_exchange_rates, uluna_exchange_rate])
    stablecoin_exchange_rates = stablecoin_exchange_rates.sort_values('denom', ascending=False).reset_index()
    stablecoin_exchange_rates = stablecoin_exchange_rates.drop(columns = ['index'])
    
    # One day is equivalent to 86400000 ms
    day_divider = 86400000
    df = pd.DataFrame(transaction_df['data'][stablecoin_index])
    df['txVolume'] = df['txVolume'].astype(float)/1000000
    df['datetime'] = (df['datetime']/day_divider).values.astype(dtype='datetime64[D]') # for day format
    df['normTxVolume'] = df['txVolume'].astype(float)/float(stablecoin_exchange_rates['amount'][stablecoin_index])
   

    return df

def get_stablecoin_exchange_rates():
    """
    This function takes no input and returns a dataframe of the current exchange rates from stablecoins to Luna
    """
    stablecoin_df = pd.DataFrame(terra.oracle.exchange_rates().to_data())
    uluna_df = pd.DataFrame([['uluna', 1]], columns = ['denom', 'amount'])
    stablecoin_df = pd.concat([stablecoin_df, uluna_df])
    stablecoin_df = stablecoin_df.sort_values('denom', ascending=False).reset_index()
    stablecoin_df = stablecoin_df.drop(columns = ['index'])
    return stablecoin_df

