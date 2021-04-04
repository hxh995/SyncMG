import urllib.request
import urllib.error
import json
import pandas as pd
import csv
from joblib import Parallel, delayed, parallel_backend
from tqdm import tqdm
from pandas import json_normalize
from utils import get_full_studies,load_studies,load_analyses,load_samples

def stat_function(args):
    #test for stat_function
    #print('the path is {}'.format(args.output_path));
    df_studies=get_full_studies('studies',args);
    load_studies(df_studies,args);
    df_analyses = get_full_studies('analyses',args);
    df_analyses=load_analyses(df_analyses);
    df_samples= get_full_studies('samples',args);
    df_samples=load_samples(df_samples);
    df = pd.merge(df_analyses, df_samples, left_on='sample', right_on='sample')
    df = df.reset_index(drop=True)
    df.to_csv(args.metadata_path,sep=',');


def diff_function(args):
    print("args_ins={},args_inf={}".format(args.input_secondary_path,args.input_first_path));
    df_inf = pd.read_csv(args.input_first_path, low_memory=False).iloc[:,1:];
    df_ins = pd.read_csv(args.input_secondary_path, low_memory=False).iloc[:,1:];
    df = df_inf.append(df_ins).drop_duplicates(['id'],keep=False);
    df = df.reset_index(drop=True)
    df.to_csv(args.output_path,sep=',');
def download_data(accession,secondary,id,args_output):
    try:
        # 将下载的内容储存成filecontents中并写入tsv文件
        req = urllib.request.urlopen('https://www.ebi.ac.uk/metagenomics/api/v1/studies/{}/pipelines/4.1/file/{}_taxonomy_abundances_SSU_v4.1.tsv'.format(accession, secondary))
        try:
            contents = req.read().decode('gb2312')  # 这里解码使用gb2313解码 如果出现乱码可以尝试改用utf-8
            # 写入本地进行保存成tsv格式
        except Exception as e:
            page = e.partial
            contents = page.decode('utf-8')
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
           # print(e.code)
            pass
        if hasattr(e, "reason"):
           # print(e.reason)
            pass
def download_function(args):
    # test
    #print("args_output={},args_threads={},args_input={}".format(args.output_path,args.threads,args.input_path));
    #对input_path 进行判断 如果有input_path 则将其赋与df
    if args.input_path:
        df=pd.read_csv(args.input_path, low_memory=False);
    #否则 其则为下载所有studies
    else:
        df=get_full_studies();
    accession = list(df['attributes.accession']);
    secondary = list(df['attributes.secondary-accession'])
    id = list(df['id'])  # 将全部的studies的4.1版本tsv下载
    par_backend = 'threads'  # {‘processes’, ‘threads’}
    par = Parallel(n_jobs=args.threads, prefer=par_backend)
    #print('Using joblib `{}` parallel backend with {} cores'.format(par_backend, args.threads))
    res = par(delayed(download_data)(accession=accession[i], secondary=secondary[i],id=id[i],args_output=args.output_path) for i in tqdm(range(len(accession))));
