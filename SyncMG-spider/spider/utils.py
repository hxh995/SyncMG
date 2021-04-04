import urllib.request
import urllib.error
import json
from functools import reduce
import re
import pandas as pd
from tqdm import tqdm
from pandas import json_normalize
from joblib import Parallel, delayed, parallel_backend
#to get the contents of pages
def read_page(url,retry_time):
    ret = ''
    for i in range(0,retry_time):
        try:
            req = json.loads(urllib.request.urlopen(url, timeout=40).read().decode('utf-8'));
            ret = req;
            break
        except:
            pass;
    return ret

#获取全部{}.api_type信息的函数
def get_full_studies(api_type,args):
        # 获取目前{}.api_type 总页数
        page_req = json.loads(urllib.request.urlopen('https://www.ebi.ac.uk/metagenomics/api/v1/{}'.format(api_type)).read().decode('utf-8'))['links']['last']
        page = int(re.search('.*?page=(\d*)', page_req).group(1));
        par_backend = 'threads';
        par = Parallel(n_jobs=args.threads, prefer=par_backend);
        #print('Using joblib `{}` parallel backend with {} cores'.format(par_backend, args.threads));
        req_pre=par(delayed(read_page)('https://www.ebi.ac.uk/metagenomics/api/v1/{1}?page={0}'.format(i,api_type),args.retry_time) for i in tqdm(range(1,page+1)));
        #test
        # req_pre = par(delayed(read_page)('https://www.ebi.ac.uk/metagenomics/api/v1/{1}?page={0}'.format(i, api_type),args.retry_time) for i in tqdm(range(1, 10)));
        # 将所有页的studies 汇总成df一个表格
        req=[i for i in req_pre if i !=''];
        df = reduce(lambda x, y: pd.concat([x, y]), map(lambda x: json_normalize(x['data']), req))
        df = df.reset_index(drop=True);
        return df;

def load_studies(df,args):
    df['pipeline_version'] = '';
    links = df['relationships.downloads.links.related']
    drop_list = list();
    for i in tqdm(range(0, len(links))):
        data = json.loads(urllib.request.urlopen(links[i]).read().decode('utf-8'))['data'];
        if data == []:
            drop_list.append(i);
        else:
            secondary = df['attributes.secondary-accession'][i];
            data = json_normalize(data);
            relationship_pipeline = data['relationships.pipeline.data.id'][
                data.id.str.contains(secondary + '_taxonomy_abundances?.*', regex=True)];
            pipeline_version = [];
            [pipeline_version.append(i) for i in list(relationship_pipeline) if not i in pipeline_version];
            df.loc[i, ['pipeline_version']] = '|'.join(pipeline_version);
    df = df.drop(drop_list);
    df = df.reset_index(drop=True)
    data = ['|'.join(j['id'] for j in i) for i in df['relationships.biomes.data']];
    df['biomes'] = data;
    df.to_csv(args.output_path, sep=',');
#deal with analyses
def load_analyses(df):
    df = df.loc[:,('relationships.run.data.id', 'relationships.sample.data.id')]
    df.rename(columns={'relationships.sample.data.id': 'sample', 'relationships.run.data.id': 'run'},inplace=True)
    return df;
#deal with samples
def load_samples(df):
    df = df.loc[:,('id', 'relationships.biome.data.id')]
    df.rename(columns={'id': 'sample', 'relationships.biome.data.id': 'biome'}, inplace=True);
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