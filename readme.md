# SyncMG

The program is desinged to download for datas on MGnify
## Installation
We recommend using git for our program
```
git clone https://github.com/hxh995/SyncMG.git
```

## quick start

```cmd
C:\Users\Dell\Desktop\ARM\spider>python SyncMG.py -h
```

## usage

```cmd
usage: SyncMG.py [-h] [-o OUTPUT_PATH] [-i INPUT_PATH] [-t THREADS]
                 [-inf INPUT_FIRST_PATH] [-ins INPUT_SECONDARY_PATH]
                 {stat,diff,download}
```

+ ```stat```

  you can use ```stat```  to check the current data on MGnify,which will return a csv file to show details of data

  ```cmd
  python SyncMG.py stat [-o]
  ```

  you can use ```-o``` to specify the path to output ,or you can check it in the 'data' file

+ ```diff```

  you can use ```diff```  to get the difference for the specified files,which will return a csv file to show the details of different studies

  ```cmd
  python SyncMG.py diff -inf xxxxx.csv -ins xxxxxx.csv [-o]
  ```

   **attention**: ```-inf```   and ```-ins```  are neccessary,on which you specify your tow files needed to operate

  you can use ```-o``` to specify the path to output ,or you can check it in the 'data' file

+ ```download```

  ```cmd
  python SyncMG.py download [-i] [-o]
  ```

  you can use this mode to download data you need 

  ```[-i]``` can be used for downloading specified data ,or you will get the whole data on MGnify

  ```[-o]```  can be used  to specify the path to output ,or you can check it in the 'download-data' file

  