import os
import requests
import urllib.request
from urllib import request
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


def download_data(accession, secondary, id, args_output,max_retry):
	url = 'https://www.ebi.ac.uk/metagenomics/api/v1/studies/{}/pipelines/4.1/file/{}_taxonomy_abundances_SSU_v4.1.tsv'.format(accession, secondary)
	filename = os.path.join(args_output, accession+'.tsv')
	ret = ''
	max_retries = max_retry
	retry = 0
	#ret, __ = request.urlretrieve(url, filename)
	while ret == '':
		try:
			ret = requests.get(url)
			with open(filename,'w') as f:
				f.write(str(ret.content, encoding='utf-8'))
			#ret, __ = request.urlretrieve(url, filename)
			print('succeeded with `{}`!'.format(accession))
			os.system('echo {} >> succeeded_list.txt'.format(accession))
		except Exception as e:
			print(e, url)
			if retry <= max_retries:
				print('failed with `{}`, still retrying...'.format(accession))
			else:
				print('Too many times of retrying, skiped!')
				os.system('echo {} >> failed_list.txt'.format(accession))
				break
			retry = retry + 1
	
'''

def download_data(accession,secondary,id,args_output):
    try:
        # ???????????????????????????filecontents????????????tsv??????
        req = urllib.request.urlopen('https://www.ebi.ac.uk/metagenomics/api/v1/studies/{}/pipelines/4.1/file/{}_taxonomy_abundances_SSU_v4.1.tsv'.format(accession, secondary))
        try:
            contents = req.read().decode('gb2312')  # ??????????????????gb2313?????? ????????????????????????????????????utf-8
            # ???????????????????????????tsv??????
        except Exception as e:
            page = e.partial
            contents = page.decode('utf-8')
        with open(args_output + '{}.tsv'.format(id), 'w') as file:
            cw = file.write(contents);
            file.close()
        # ?????????csv?????? ????????????
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
'''


def download_function(args):
    # test
    #print("args_output={},args_threads={},args_input={}".format(args.output_path,args.threads,args.input_path));
    #???input_path ???????????? ?????????input_path ???????????????df
    if args.input_path:
        df=pd.read_csv(args.input_path, low_memory=False);
    #?????? ?????????????????????studies
    else:
        df=get_full_studies();
    accession = list(df['attributes.accession']);
    secondary = list(df['attributes.secondary-accession'])
    id = list(df['id'])  # ????????????studies???4.1??????tsv??????
    par_backend = 'threads'  # {???processes???, ???threads???}
    par = Parallel(n_jobs=args.threads, prefer=par_backend)
    #print('Using joblib `{}` parallel backend with {} cores'.format(par_backend, args.threads))
    res = par(delayed(download_data)(accession=accession[i], secondary=secondary[i],id=id[i],args_output=args.output_path,max_retry=args.retry_time) for i in tqdm(range(len(accession))));
