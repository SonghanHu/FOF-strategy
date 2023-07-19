from rqalpha_plus.apis import *
import pandas as pd
from rqdatac import *
import rqdatac

rqdatac.init()
import numpy as np
import rqdatac_fund
from datetime import datetime


# 在这个方法中编写任何的初始化逻辑。context对象将会在你的算法策略的任何方法之间做传递。

def init(context):
    context.etf_list = ['513230.XSHG',
 '513660.XSHG',
 '510630.XSHG',
 '515170.XSHG',
 '516650.XSHG',
 '159711.XSHE',
 '159601.XSHE',
 '515010.XSHG',
 '516500.XSHG',
 '159995.XSHE',
 '159732.XSHE',
 '159983.XSHE',
 '510050.XSHG',
 '159627.XSHE',
 '516190.XSHG',
 '512770.XSHG',
 '512990.XSHG',
 '510650.XSHG',
 '510330.XSHG',
 '159957.XSHE',
 '515020.XSHG',
 '510660.XSHG',
 '516710.XSHG',
 '515050.XSHG',
 '517170.XSHG',
 '159635.XSHE',
 '159902.XSHE',
 '159967.XSHE',
 '159726.XSHE',
 '512500.XSHG',
 '159783.XSHE',
 '159666.XSHE',
 '159790.XSHE',
 '159966.XSHE',
 '159731.XSHE',
 '159791.XSHE',
 '516100.XSHG',
 '159758.XSHE',
 '562530.XSHG',
 '516260.XSHG',
 '588000.XSHG',
 '159845.XSHE',
 '515030.XSHG',
 '516320.XSHG',
 '515760.XSHG',
 '159620.XSHE',
 '516850.XSHG',
 '159617.XSHE',
 '512950.XSHG',
 '516810.XSHG',
 '159869.XSHE',
 '515060.XSHG',
 '562520.XSHG',
 '515070.XSHG',
 '562510.XSHG',
 '562550.XSHG',
 '159888.XSHE',
 '562500.XSHG',
 '516630.XSHG',
 '159663.XSHE',
 '516000.XSHG']

    context.rebalance_period = 20
    context.counter = 0
    context.cash = 100000000


def handle_bar(context, bar_dict):
    # Get the current and next trading date
    current_trading_date = context.now
    next_trading_date = get_next_trading_date(current_trading_date)

    # Check if the current trading date is the last trading day of the month
    if current_trading_date.month != next_trading_date.month:

        for etf in context.portfolio.positions:
            order_target_percent(etf, 0)

        signals_df = calculate_signals(context.etf_list, current_trading_date.strftime('%Y-%m-%d'))
        selected_etfs = select_etfs(signals_df)

        weights = []
        for etf_code in selected_etfs:
            weight = kelly_equation(etf_code, current_trading_date)
            weights.append(weight)

        summ = sum(weights)
        print(weights)
        print(selected_etfs)
        for num in range(len(selected_etfs)):
            weight = weights[num] / summ
            if isinstance(weight, float):
                if weight >= 0:
                    order_value(selected_etfs[num], weight * context.cash)
                else:
                    continue
            else:
                order_value(selected_etfs[num], 1 / len(selected_etfs) * context.cash)

        portfolio_df = pd.DataFrame.from_dict(context.portfolio.positions, orient='index')
        # portfolio_df.to_excel(f'portfolio/portfolio_{current_trading_date.strftime("%Y-%m-%d")}.xlsx')

        print(current_trading_date.strftime('%Y-%m-%d'))


def select_etfs(signals_df):
    # Exclude ETFs with negative PE ratios
    signals_df = signals_df[signals_df['PE Ratio'] > 0]

    # Select the top 30 ETFs based on monthly returns
    select1 = signals_df.sort_values('Monthly Return', ascending=False).head(30)

    # select1 = select1.loc[
    # [i for i in select1.index if (select1.loc[i, 'Current Price'] > select1.loc[i, 'Average Price'])], :]

    # From those, select the top 10 ETFs with the lowest PE ratio
    select2 = select1.sort_values('PE Ratio', ascending=True).head(10).reset_index(drop=True)
    selected_etfs = select2['ETF Code']
    # If the selected ETFs have negative returns, replace them with 511010.XSHE
    # selected_etfs = select2['ETF Code'].apply(
    #    lambda x: x if select2[select2['ETF Code'] == x]['Monthly Return'].item() > 0
    #    # or (select2[select2['ETF Code'] == x]['Monthly Return'].item() < -0.006)
    #    else '511010.XSHG').tolist()

    return selected_etfs


def calculate_signals(etf_list, trading_date):
    # Convert trading_date to datetime object.
    trading_date = datetime.strptime(trading_date, '%Y-%m-%d')

    signals = []
    for etf_code in etf_list:
        monthly_return = calculate_monthly_return(etf_code, trading_date)
        pe_ratio = calculate_etf_pe_ratio(etf_code, trading_date)
        try:
            current_price = history_bars(etf_code, 1, '1d', 'close').item()
        except:
            current_price = None
        average_price = calculate_average_price(etf_code, trading_date)
        signals.append([etf_code, monthly_return, pe_ratio, current_price, average_price])

    signals_df = pd.DataFrame(signals, columns=['ETF Code',
                                                'Monthly Return',
                                                'PE Ratio',
                                                'Current Price',
                                                'Average Price'])

    return signals_df


def calculate_etf_pe_ratio(etf_code, trading_date):
    if check_listing_date(etf_code, trading_date):
        df = rqdatac.fund.get_etf_components(etf_code, trading_date=trading_date).reset_index(drop=True)
        sec_income = 0
        sec_market_cap = 0
        for i in df.index:
            try:
                foundamental = get_factor(df.loc[i, 'stock_code'], ['pe_ratio', 'market_cap'])
                pe_stock = foundamental['pe_ratio'].item()
                market_cap_stock = foundamental['market_cap'].item()
                if pe_stock == 0:
                    pass
                else:
                    sec_income += market_cap_stock / pe_stock
                    sec_market_cap += market_cap_stock
            except:
                pass
        if sec_income != 0 and sec_market_cap != 0:
            final_pe = sec_market_cap / sec_income
            return final_pe
        else:
            return None
    else:
        return None


def calculate_monthly_return(etf_code, trading_date):
    if check_listing_date(etf_code, trading_date):
        price_data = history_bars(etf_code, 21, '1d', 'close')
        price_data = pd.DataFrame(price_data, columns=['close'])
        price_data['return'] = price_data['close'].pct_change()
        average_return = price_data['return'].mean()
        return average_return
    else:
        return None


def calculate_average_price(etf_code, trading_date):
    if check_listing_date(etf_code, trading_date):
        price_data = history_bars(etf_code, 21, '1d', 'close')
        price_data = pd.DataFrame(price_data, columns=['close'])
        average_price = price_data['close'].mean()
        return average_price
    else:
        return None


def check_listing_date(order_book_id, datetime_obj):
    instrument = instruments(order_book_id)
    if instrument is not None:
        # Extract date from datetime.datetime object
        if instrument.listed_date <= datetime_obj:
            return True
    return False


def kelly_equation(etf_code, trading_date):
    weight = 1 / 10

    if check_listing_date(etf_code, trading_date):
        price_data = history_bars(etf_code, 21, '1d', 'close')
        price_data = pd.DataFrame(price_data, columns=['close'])
        price_data['return'] = price_data['close'].pct_change()

        # Number of returns
        num_returns = len(price_data['return'].dropna())

        # Calculate the probability of win and lose
        proba_win = len(price_data[price_data['return'] > 0]) / num_returns
        proba_loss = len(price_data[price_data['return'] < 0]) / num_returns

        # Calculate odds: average winning return divided by average absolute losing return
        odds = price_data[price_data['return'] > 0]['return'].mean() / abs(
            price_data[price_data['return'] < 0]['return'].mean())
        weight = proba_win - (proba_loss / odds)

    return weight


config = {
    "base": {
        "accounts": {
            "STOCK": 100000000,
        },
        "start_date": "20191228",
        "end_date": "20230701",
    },
    "mod": {
        "sys_analyser": {
            "plot": True,
            "benchmark": "000905.XSHG"
        },
        "sys_simulation": {
            "volume_limit": False,
        }
    }
}

if __name__ == "__main__":
    from rqalpha_plus import run_func

    run_func(config=config, init=init, handle_bar=handle_bar)
