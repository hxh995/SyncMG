import urllib.request
import urllib.error
import json
from functools import reduce
import re
import pandas as pd
from tqdm import tqdm
from pandas import json_normalize
#获取全部studies 信息的函数
def get_full_studies():
# 获取目前studies总页数
        page_req = json.loads(urllib.request.urlopen('https://www.ebi.ac.uk/metagenomics/api/v1/studies').read().decode('utf-8'))['links']['last']
        page = re.search('.*?page=(\d*)', page_req).group(1)
        # 下载学习页的json文件
        req=[json.loads(urllib.request.urlopen('https://www.ebi.ac.uk/metagenomics/api/v1/studies?page={}'.format(i)).read().decode('utf-8')) for i in tqdm(range(1,page+1))];
        #req = [json.loads(urllib.request.urlopen('https://www.ebi.ac.uk/metagenomics/api/v1/studies?page={}'.format(i)).read().decode('utf-8')) for i in tqdm(range(4, 10))];
        # 将所有页的studies 汇总成df一个表格
        df = reduce(lambda x, y: pd.concat([x, y]), map(lambda x: json_normalize(x['data']), req))
        df = df.reset_index(drop=True);
        return df;
#在当前路径下创造指定的目录
def mkdir(path):
    import os
    #去除首位空格
    path=path.strip()
    #去除尾部 \ 符号
    path=path.rstrip('\\')
    #判断路径是否存在 如果已经存在 则返回True 否则为False
    isExists = os.path.exists(path)
    #如果不存在则创建目录 否则不操作
    if not isExists:
        os.makedirs(path)