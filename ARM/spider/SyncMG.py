import argparse
import mode
from utils import mkdir
parser= argparse.ArgumentParser();
import datetime

# 初始化限制mode 本质是占位置参数
parser.add_argument("mode",type=str,choices=['stat','diff','download'],default='',help='choose a mode to use the program.default:None')
#增加output-dir的参数 for mode=stat/diff/downloads
parser.add_argument("-o","--output_path",type=str,default="",help='path to store your output file')
##增加metadata_path的参数 for mode=stat
parser.add_argument("-m","--metadata_path",type=str,default="",help='path to store your metadata/label file')
#增加input-dir的参数
# for mode=download
parser.add_argument("-i","--input_path",default="",type=str,help="path to input your csv file in downloads modes");
parser.add_argument("-t","--threads",type=int,default=1,help="choose the numbers of threads for your downloading.default:1")
# for mode = diff
parser.add_argument("-inf","--input_first_path",default="",type=str,help="path to input your first csv file in diff mode. default:None")
parser.add_argument("-ins","--input_secondary_path",default="",type=str,help="path to input second csv file in diff mode.default:Node")
args=parser.parse_args();
# 进行mode 的占位语法检查
# print(args)
if args.mode in ['stat','diff','download']:
    #打印正在使用的mode
    print('{} mode is working'.format(args.mode))
    #stat 逻辑部分
    if args.mode=='stat':
        if args.output_path:
            print("output_path={}".format(args.output_path));
        else:
            timefornow= datetime.datetime.now().strftime('%Y-%m-%d')
            mkdir('data/');
            args.output_path='data/stat-'+ timefornow + '.csv'
            print("output_path is missing,the path is {}".format(args.output_path));
        if args.metadata_path:
            print("metadata_path={}".format(args.metadata_path));
        else:
            timefornow = datetime.datetime.now().strftime('%Y-%m-%d')
            mkdir('data/');
            args.metadata_path = 'data/metadata-' + timefornow + '.csv'
            print("metadata_path is missing,the path is {}".format(args.metadata_path));
        mode.stat_function(args);
    elif args.mode=='diff':
        if (args.input_first_path and args.input_secondary_path) :
                if args.output_path:
                    print("output_path={}".format(args.output_path));
                else:
                    timefornow = datetime.datetime.now().strftime('%Y-%m-%d');
                    mkdir('data/');
                    args.output_path = 'data/diff-' + timefornow + '.csv'
                    print("output_path is missing,the path is {}".format(args.output_path));
                mode.diff_function(args)
        else:
            print("necessary arguments are missing,type -h to get help");
    else:
        if args.input_path:
            print("input_path={},the program will only download the specified studies in the input file".format(args.input_path));
        else:
            print("the program will download the whole studies")
        if args.output_path:
            print("output_path={}".format(args.output_path));
        else:
            #未指定输出文件夹，对默认判断文件夹进行判断，不存在则创建
            args.output_path='download-data/'
            mkdir(args.output_path);
            print("output_path is missing,the path is {}".format(args.output_path));
        mode.download_function(args)


else:
    print("Must secify a vaild work mode,type -h to check the help information")
#开始进行mode 的逻辑部分分析，这里为了避免代码冗余 不写入上述的if内


# print(args.stats)