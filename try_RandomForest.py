import rqdatac
from rqalpha_plus.apis import *
import pandas as pd
from rqdatac import *
import rqdatac_fund
from dateutil.relativedelta import relativedelta
import datetime
import traceback
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor

rqdatac.init()


def re_balance(context, bar_dict):
    global to_buy_list
    current_trading_date = context.now
    next_trading_date = get_next_trading_date(current_trading_date)

    # Increment the month counter if we're crossing into a new month
    if current_trading_date.month != next_trading_date.month:
        context.months_since_rebalance += 1

    # Re-balance every 3 months or on the first trade
    if context.months_since_rebalance == 3 or context.first_trade:
        position_ids = [position.order_book_id for position in get_positions()]

        # Sell the old funds at the end of the current month
        if get_next_trading_date(current_trading_date).month != current_trading_date.month:
            for order_book_id in position_ids:
                order_target_percent(order_book_id, 0)

        # Buy the new funds at the start of the next month
        elif get_previous_trading_date(get_previous_trading_date(
                get_previous_trading_date(current_trading_date
                                          ))).month != current_trading_date.month and get_previous_trading_date(
            get_previous_trading_date(current_trading_date
                                      )).month == current_trading_date.month:
            to_buy = rank(context, fund_list=context.fund_list, day=str(current_trading_date))
            to_buy_list = to_buy.index
            for order_book_id in to_buy_list[:50]:
                if order_book_id not in position_ids:
                    order_target_percent(order_book_id, 1 / 50)
            context.first_trade = False
            context.months_since_rebalance = 0
    try:
        num = 1
        while context.portfolio.cash > 100000 and num < 20:
            order_target_percent(to_buy_list[num], 1 / 50)
            num += 1
    except:
        pass

        # positions = context.portfolio.positions
        # data = []
        # for order_book_id, position in positions.items():
        #     data.append({
        #         'order_book_id': order_book_id,
        #         'quantity': position.quantity,
        #         'avg_price': position.avg_price,
        #         'market_value': position.market_value
        #     })
        # portfolio_df = pd.DataFrame(data)


#
# portfolio_df.to_excel(f'portfolio/portfolio_{current_trading_date.strftime("%Y-%m-%d")}.xlsx', index=False)


def init(context):
    context.fund_list = get_fund_list(context)
    context.factors = get_factors_list()
    context.months_since_rebalance = 0
    context.first_trade = 1
    scheduler.run_monthly(re_balance, tradingday=1)


def get_fund_list(context):
    fund_data = fund.all_instruments(date=str(context.now))
    fund_requirement = ['普通股票型', '偏股混合', '平衡混合', '灵活配置']
    fund_list = []
    for i in range(len(fund_data)):
        for j in fund_requirement:
            if (j in fund_data.symbol[i]) or (fund_data.fund_type[i] == 'Stock'):
                fund_list.append(fund_data.order_book_id[i])

    fund_list.remove('150050')
    return list(set(fund_list))


def get_factors_list():
    return ['m3_return_x', 'm3_benchmark_return', 'm3_stdev_a',
            'm3_dev_downside_avg_a', 'm3_dev_downside_rf_a', 'm3_mdd',
            'm3_excess_mdd', 'm3_mdd_days', 'm3_max_drop', 'm3_max_drop_period',
            'm3_neg_return_ratio', 'm3_kurtosis', 'm3_skewness',
            'm3_tracking_error', 'm3_beta_downside', 'm3_beta_upside', 'm3_var',
            'm3_alpha_a', 'm3_alpha_tstats', 'm3_beta', 'm3_sharpe_a', 'm3_inf_a',
            'm3_sortino_a', 'm3_calmar_a', 'm3_timing_ratio', 'intercept']


def handle_bar(context, bar_dict):
    print(context.portfolio.cash)
    try:
        re_balance(context, bar_dict)
    except Exception as e:
        print(e)
        print(traceback.format_exc())


def reg(context, indicators, coef):
    indicators['intercept'] = 1
    predicted_return = 0
    for factor in context.factors:
        predicted_return += indicators[factor] * coef[factor]
    return predicted_return


def get_factors(fund_list, date_x):
    indicators = rqdatac_fund.fund.get_indicators(fund_list, start_date=date_x, end_date=date_x)
    indicators_m3 = indicators[[i for i in indicators.columns if 'm3' in i]].reset_index(drop=False)
    indicators_m3 = indicators_m3.drop(['m3_recovery_days', 'datetime', 'm3_return_a',
                                        'm3_excess', 'm3_excess_a', 'm3_excess_win'],
                                       axis=1).rename({'m3_return': 'm3_return_x'}, axis=1)
    indicators_m3 = indicators_m3.set_index('order_book_id')

    return indicators_m3


def rank(context, fund_list, day):
    factors = get_factors(fund_list=fund_list, date_x=day)
    return_dict = {}
    for i in factors.index:
        try:
            return_dict[i] = reg1.predict(factors.loc[[i]])
        except Exception as e:
            return_dict[i] = -100

    return_series = pd.Series(return_dict)
    return_series.sort_values(ascending=False, inplace=True)
    return return_series


def fund_get_data(fund_list, date):

    date_mid = datetime.datetime.strptime(date, '%Y-%m-%d')
    dateX = (date_mid - relativedelta(months=3)).strftime('%Y-%m-%d')
    trading_day_X = rqdatac.get_trading_dates(start_date=dateX, end_date=dateX)
    while not trading_day_X:
        dateX = str((datetime.datetime.strptime(dateX, '%Y-%m-%d') - relativedelta(days=1)).strftime('%Y-%m-%d'))
        trading_day_X = rqdatac.get_trading_dates(start_date=dateX, end_date=dateX)
    dateX = trading_day_X[0].strftime('%Y-%m-%d')

    performance = rqdatac_fund.fund.get_indicators(fund_list, start_date=date, end_date=date,
                                                   fields=['m3_return', 'm3_benchmark_return']).reset_index(drop=False)
    performance = performance.drop('datetime', axis=1).rename(
        {'m3_return': 'm3_return_y', 'm3_benchmark_return': 'm3_benchmark_return_y'}, axis=1)

    indicators = rqdatac_fund.fund.get_indicators(fund_list, start_date=dateX, end_date=dateX)
    indicators_m3 = indicators[[i for i in indicators.columns if 'm3' in i]].reset_index(drop=False)
    indicators_m3 = indicators_m3.drop(['m3_recovery_days', 'datetime', 'm3_return_a',
                                        'm3_excess', 'm3_excess_a', 'm3_excess_win', ], axis=1).rename(
        {'m3_return': 'm3_return_x'}, axis=1)
    indicators_m3 = indicators_m3.loc[[j for j in indicators_m3.index if ~indicators_m3.loc[j].isna().any()]]
    df_returned = performance.merge(indicators_m3, on='order_book_id')

    return df_returned


def get_full(fund_list, end_date_str):
    if ':' in end_date_str:
        end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d %H:%M:%S')
    else:
        end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d')
    start_date = end_date - relativedelta(years=5)
    frequency = pd.date_range(start=start_date + pd.DateOffset(months=3), end=end_date, freq='3M')
    concatenated_df = pd.DataFrame()

    for date in frequency:
        date_str = date.strftime('%Y-%m-%d')
        trading_day = rqdatac.get_trading_dates(start_date=date_str, end_date=date_str)
        while not trading_day:
            date = date - relativedelta(days=1)
            date_str = date.strftime('%Y-%m-%d')
            trading_day = rqdatac.get_trading_dates(start_date=date_str, end_date=date_str)
        df = fund_get_data(fund_list, date=trading_day[0].strftime('%Y-%m-%d'))
        concatenated_df = pd.concat([concatenated_df, df], axis=0)

    return concatenated_df.dropna()


fund_data = rqdatac_fund.fund.all_instruments(date=str(datetime.datetime.now())[:10])
fund_requirement = ['普通股票型', '偏股混合', '平衡混合', '灵活配置']
fund_list = []
for i in range(len(fund_data)):
    for j in fund_requirement:
        if (j in fund_data.symbol[i]) or (fund_data.fund_type[i] == 'Stock'):
            fund_list.append(fund_data.order_book_id[i])

fund_list.remove('150050')

data = get_full(fund_list, str(datetime.datetime.now())[:10])
reg1 = RandomForestRegressor(n_estimators=250, max_leaf_nodes=50, max_depth=25)
reg1.fit(data[data.columns[3:]], data['m3_return_y'])

config = {
    "base": {
        "accounts": {
            "STOCK": 10000000,
        },
        "start_date": "20210125",
        "end_date": "20230701",
    },
    "mod": {
        "sys_analyser": {
            "plot": True,
            "benchmark": "000300.XSHG"
        },
        "sys_simulation": {
            "volume_limit": False
        }
    }
}

if __name__ == "__main__":
    from rqalpha_plus import run_func

    run_func(config=config, init=init, handle_bar=handle_bar)
