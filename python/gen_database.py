'''
用于生成基于litesql的database
使用方法 python gen_database.py
'''
import zipfile,os
from zipfile import BadZipFile
import pandas
from sqlitedict import SqliteDict
from datetime import datetime

cache = SqliteDict("backtest_cache_1m.db")


class GenDatabase:
    def __init__(self) -> None:
        self.monthly_klines_relpath = "data/futures/um/monthly/markPriceKlines"
        for root, dirs, files in os.walk(self.monthly_klines_relpath):
            for file in files:
                print(file)
                if ".zip" in file and file.replace("zip", "csv") not in files:
                    self.unzip(os.path.join(root, file), root)
                else:
                    print(f"unzip {file} already, skip...")

        for root, dirs, files in os.walk(self.monthly_klines_relpath):
            for file in files:
                if ".csv" in file:
                    self.parse_csv_to_database(os.path.join(root, file))

    def unzip(self, zipname, dst):
        try:
            with zipfile.ZipFile(zipname, "r") as zip_ref:
                zip_ref.extractall(dst)
                print(f"unzip:{zipname}")
        except BadZipFile as e:
            print(f"{zipname} is not a zip file, please download again!!!!!!!!")

    def parse_csv_to_database(self, csvname: str):
        '''
        Open time	Open	High	Low	Close	Volume	Close time	Quote asset volume	Number of trades	Taker buy base asset volume	Taker buy quote asset volume	Ignore
        database 格式 key: BTCUSDT - datetime.strftime("%d %b %Y %H:%M:%S")
        '''
        symbol = os.path.basename(csvname)

        if cache.get(f"{symbol}-end") == 1: # 如果该文件已经转换完毕了则跳过
            print(f"{symbol}已经转换完毕，跳过")
            return

        # 检查csv文件类型，保证第一行不是列名称
        lines = []
        with open(csvname, "r") as f:
            line = f.readline()
            if "open" in line:
                lines = f.readlines()
        if lines:
            with open(csvname, "w") as f:
                f.writelines(lines[1:])

        cache[f"{symbol}-start"] = 1 # 标志该文件开始
        df = pandas.read_csv(csvname, prefix="_c", header=None)
        print(f"f{csvname} 转database开始")
        for i in range(len(df)):
            timestamp = df["_c0"][i]
            price = (df["_c2"][i] + df["_c3"][i]) / 2
            _date = datetime.fromtimestamp(int(timestamp/1000))
            cache[f"{symbol}USDT - {_date.strftime('%d %b %Y %H:%M:%S')}"] = price
            # print(_date, price)
        cache[f"{symbol}-end"] = 1 # 标志该文件完成
        cache.commit()
        print(f"f{csvname} 转database完成")

if __name__ == "__main__":
    GenDatabase()
