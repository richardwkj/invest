import FinanceDataReader as fdr
# import pandas as pd
# import numpy as np
# import datetime
# import matplotlib.pyplot as plt
# import seaborn as sns

df = fdr.DataReader('AAPL', '2020')
print(df.head())  # 데이터의 처음 몇 줄을 출력합니다.

