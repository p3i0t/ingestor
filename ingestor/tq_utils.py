from contextlib import contextmanager, closing
import datetime
import time
import os

import polars as pl
from tqsdk import TqApi, TqAuth
from tqsdk.tools import DataDownloader


@contextmanager
def get_tq_api():
    api = TqApi(auth=TqAuth('13691845749', '60Pacmer')) 
    try:
        yield api
    finally:
        api.close()


def _get_cont_tick_of_range(symbol: str, start: datetime.date, end: datetime.date, dir: str):
    #    with get_tq_api() as api:
    api = TqApi(auth=TqAuth('13691845749', '60Pacmer')) 
    df = api.get_trading_calendar(start_dt=start, end_dt=end)
    tlist = [dd.date() for dd in df[df['trading']]['date'].to_list()]

    # 查询某主力合约（快期）代码的历史主力合约代码
    conts = api.query_his_cont_quotes(symbol=[symbol], n=len(tlist))
    conts = pl.from_pandas(conts)
    
    conts = conts.filter(
        pl.col('date').cast(pl.Date).gt(start) 
        & pl.col('date').cast(pl.Date).lt(end)
        )
        
    from collections import defaultdict
    cont_dates_dict = defaultdict(list)
    for _date, _symbol in conts.iter_rows():
        if _symbol == "":
            continue
        cont_dates_dict[_symbol].append(_date.date())

    # 根据实际主力合约代码和日期分组下载tick数据（这样得到的才是5档行情，如果有的话）
    # cont_df_list = []
    tasks = {}
    for cont_symbol, dates in cont_dates_dict.items():
        start = min(dates)
        end = max(dates)
        try:
            os.makedirs(dir, exist_ok=False)
        except OSError:
            print(f"Directory {dir} already exists, task done before.")
            return
        tasks[cont_symbol] = DataDownloader(
            api, symbol_list=cont_symbol, dur_sec=0, 
            start_dt=start, end_dt=end, 
            csv_file_name=os.path.join(dir, f"{cont_symbol}.csv"))
        
    with closing(api):
        while not all([v.is_finished() for v in tasks.values()]):
            api.wait_update()
            time.sleep(5)
            print("progress: ", { k:("%.2f%%" % v.get_progress()) for k,v in tasks.items() })
            
        #     df = api.get_tick_data_series(
        #         symbol=cont_symbol, start_dt=start, end_dt=end
        #     )   
        #     df = pl.from_pandas(df)
        #     cont_df_list.append(df)
        # df = pl.concat(cont_df_list)
        # # df = df.with_columns(
        # #     pl.col('datetime').cast(
        # #         pl.Datetime('ns', time_zone='Asia/Shanghai')).dt.replace_time_zone(None).alias('datetime'))
        # return df 