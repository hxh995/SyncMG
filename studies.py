import urllib.request
import urllib.error
import json
from functools import reduce
import pandas as pd
import csv
import re
#获取目前studies总页数
page_req=json.loads(urllib.request.urlopen('https://www.ebi.ac.uk/metagenomics/api/v1/studies').read().decode('utf-8'))['links']['last']
page=re.search('.*?page=(\d*)',page_req).group(1)
#下载学习页的json文件
# req=[json.loads(urllib.request.urlopen('https://www.ebi.ac.uk/metagenomics/api/v1/studies?page={}'.format(i)).read().decode('utf-8')) for i in range(1,page+1)];
req=[json.loads(urllib.request.urlopen('https://www.ebi.ac.uk/metagenomics/api/v1/studies?page={}'.format(i)).read().decode('utf-8')) for i in range(1,5)];
#将所有页的studies 汇总成df一个表格
df=reduce(lambda x,y:pd.concat([x,y]),map(lambda x:pd.json_normalize(x['data']),req))
#得到所有的序列号和版本号用于后继4.1downloads下载
accession=list(df['attributes.accession']);
secondary=list(df['attributes.secondary-accession'])
id=list(df['id'])
#将全部的studies的4.1版本tsv下载
#for i in range(0,len(accession)):
for i in range(0,15):
    try:
        #将下载的内容储存成filecontents中并写入tsv文件
        req=urllib.request.urlopen('https://www.ebi.ac.uk/metagenomics/api/v1/studies/{}/pipelines/4.1/file/{}_taxonomy_abundances_SSU_v4.1.tsv'.format(accession[i],secondary[i]))
        contents=req.read().decode('gb2312')#这里解码使用gb2313解码 如果出现乱码可以尝试改用utf-8
        #写入本地进行保存成tsv格式
        with open('{}.tsv'.format(id[i]),'w') as file:
            cw = file.write(contents);
            file.close()
        #转换成csv格式 以便备用
        with open('{}.tsv'.format(id[i]), 'r') as fin:
            cr = csv.reader(fin, delimiter='\t')
            filecontents = [line for line in cr]

        with open('{}.csv'.format(id[i]), 'w') as fou:
            cw = csv.writer(fou, quotechar='', quoting=csv.QUOTE_NONE, escapechar='\\')
            cw.writerows(filecontents)
            fou.close()
    except urllib.error.URLError as e:
            if hasattr(e,"code"):
                print (e.code)
            if hasattr(e, "reason"):
                print (e.reason)




