import urllib.request
import urllib.error
import json
from functools import reduce
import re
import pandas as pd
import csv
from joblib import Parallel, delayed, parallel_backend
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

def stat_function(args_outputpath):
    #test for stat_function
    print('the path is {}'.format(args_outputpath));
    # downloads for the current data of all studies
    # 获取目前studies总页数
    page_req = json.loads(urllib.request.urlopen('https://www.ebi.ac.uk/metagenomics/api/v1/studies').read().decode('utf-8'))['links']['last']
    page = re.search('.*?page=(\d*)', page_req).group(1)
    #检测以下是否将page都爬下来了
    # print(page)
    #下载目前所有学习页的json文件
    # req=[json.loads(urllib.request.urlopen('https://www.ebi.ac.uk/metagenomics/api/v1/studies?page={}'.format(i)).read().decode('utf-8')) for i in range(1,page+1)];
    #下载 5页studies 项目进行test
    req = [json.loads(urllib.request.urlopen('https://www.ebi.ac.uk/metagenomics/api/v1/studies?page={}'.format(i)).read().decode('utf-8')) for i in range(1, 6)];
    df = reduce(lambda x, y: pd.concat([x, y]), map(lambda x: pd.json_normalize(x['data']), req))
    df = df.reset_index(drop=True)
    links=df['relationships.downloads.links.related']
    for i in range(0, len(links)):
        if json.loads(urllib.request.urlopen(links[i]).read().decode('utf-8'))['data'] == []:
            df=df.drop(i)
    df = df.reset_index(drop=True)
    df.to_csv(args_outputpath,sep=',');
def diff_function(args_inf,args_ins,args_outputpath):
    print("args_ins={},args_inf={}".format(args_ins,args_inf));
    df_inf = pd.read_csv(args_inf, low_memory=False).iloc[:,1:];
    df_ins = pd.read_csv(args_ins, low_memory=False).iloc[:,1:];
    df = df_inf.append(df_ins).drop_duplicates(['id'],keep=False);
    df = df.reset_index(drop=True)
    df.to_csv(args_outputpath,sep=',');
def download_data(accession,secondary,id,args_output):
    try:
        # 将下载的内容储存成filecontents中并写入tsv文件
        req = urllib.request.urlopen('https://www.ebi.ac.uk/metagenomics/api/v1/studies/{}/pipelines/4.1/file/{}_taxonomy_abundances_SSU_v4.1.tsv'.format(accession, secondary))
        contents = req.read().decode('gb2312')  # 这里解码使用gb2313解码 如果出现乱码可以尝试改用utf-8
        # 写入本地进行保存成tsv格式
        with open(args_output + '{}.tsv'.format(id), 'w') as file:
            cw = file.write(contents);
            file.close()
        # 转换成csv格式 以便备用
        with open(args_output + '{}.tsv'.format(id), 'r') as fin:
            cr = csv.reader(fin, delimiter='\t')
            filecontents = [line for line in cr]

        with open(args_output + '{}.csv'.format(id), 'w') as fou:
            cw = csv.writer(fou, quotechar='', quoting=csv.QUOTE_NONE, escapechar='\\')
            cw.writerows(filecontents)
            fou.close()
    except urllib.error.URLError as e:
        if hasattr(e, "code"):
            print(e.code)
        if hasattr(e, "reason"):
            print(e.reason)
def download_function(args_output,args_input,args_threads):
    print("args_output={},args_threads={},args_input={}".format(args_output,args_threads,args_input));
    #对input_path 进行判断 如果有input_path 则将其赋与df
    if args_input:
        df=pd.read_csv(args_input, low_memory=False);
    #否则 其则为下载所有studies
    else:
        # 获取目前studies总页数
        page_req = json.loads(urllib.request.urlopen('https://www.ebi.ac.uk/metagenomics/api/v1/studies').read().decode('utf-8'))['links']['last']
        page = re.search('.*?page=(\d*)', page_req).group(1)
        # 下载学习页的json文件
        # req=[json.loads(urllib.request.urlopen('https://www.ebi.ac.uk/metagenomics/api/v1/studies?page={}'.format(i)).read().decode('utf-8')) for i in range(1,page+1)];
        req = [json.loads(urllib.request.urlopen('https://www.ebi.ac.uk/metagenomics/api/v1/studies?page={}'.format(i)).read().decode('utf-8')) for i in range(1, 5)];
        # 将所有页的studies 汇总成df一个表格
        df = reduce(lambda x, y: pd.concat([x, y]), map(lambda x: pd.json_normalize(x['data']), req))
    accession = list(df['attributes.accession']);
    secondary = list(df['attributes.secondary-accession'])
    id = list(df['id'])  # 将全部的studies的4.1版本tsv下载
    # args_output=[args_output]*len(accession);
    par_backend = 'threads'  # {‘processes’, ‘threads’}
    par = Parallel(n_jobs=args_threads, prefer=par_backend)
    print('Using joblib `{}` parallel backend with {} cores'.format(par_backend, args_threads))
    res = par(delayed(download_data)(accession=accession[i], secondary=secondary[i],id=id[i],args_output=args_output) for i in range(len(accession)))
