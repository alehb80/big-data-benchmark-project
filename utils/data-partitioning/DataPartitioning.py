import pandas as pd

col_list = ["ticker", "open", "close", "adj_close", "low", "high", "volume", "date"]
df = pd.read_csv("../../contents/historical_stock_prices.csv", usecols=col_list)

half_size = int(20973889/2)
one_third_size = int(20973889/3)

half_df = df.sample(half_size)
one_third_df = df.sample(one_third_size)

half_df.to_csv('../../contents/partitioned-data/historical_stock_prices_half.csv', index=False)
one_third_df.to_csv('../../contents/partitioned-data/historical_stock_prices_one_third.csv', index=False)
