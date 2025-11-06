import torch
import torch.nn as nn
import torch.optim as optim
import torch.nn.functional as F
import numpy as np
import pandas as pd
import os
from tqdm import tqdm

pd.read_csv("./data/2017_Q1/2017-01-01.csv").keys()

import os
import pandas as pd
from tqdm import tqdm
import gc
import tempfile

# 定义要处理的目录
directories = [
    "./data/2017_Q1/",
    "./data/2017_Q2/",
    "./data/2017_Q3/"
]

# SMART 5: 重映射扇区计数
# SMART 9: 通电时间累计
# SMART 187: 无法校正的错误
# SMART 188: 指令超时计数
# SMART 193: 磁头加载/卸载计数
# SMART 194: 温度
# SMART 197: 等待被映射的扇区数
# SMART 198: 报告给操作系统的无法通过硬件ECC校正的错误
# SMART 241: 逻辑块寻址模式写入总数
# SMART 242: 逻辑块寻址模式读取总数
# 需要保留的列 5、187、188、197、198
columns_to_keep = ['date', 'serial_number', 'model', 'capacity_bytes','failure','smart_197_normalized', \
                   'smart_5_normalized', 'smart_187_normalized', 'smart_188_normalized',\
                   'smart_193_normalized', 'smart_194_normalized', \
                   'smart_241_normalized', 'smart_9_normalized']  

# 数据类型优化 - 根据实际数据范围调整


# 创建临时目录存储中间文件
with tempfile.TemporaryDirectory() as temp_dir:
    temp_files = []
    
    # 处理每个目录
    for dir_path in directories:
        if not os.path.exists(dir_path):
            print(f"目录 {dir_path} 不存在，跳过")
            continue
            
        # 获取目录中所有CSV文件的列表
        csv_files = [f for f in os.listdir(dir_path) if f.endswith(".csv")]
        
        # 处理目录中的每个CSV文件
        for file in tqdm(csv_files, desc=f"处理 {dir_path}"):
            file_path = os.path.join(dir_path, file)
            
            try:
                # 读取CSV时进行优化
                df = pd.read_csv(
                    file_path,
                    usecols=columns_to_keep,
                    parse_dates=['date'],
                    low_memory=True
                )
                
                # 生成临时文件路径
                temp_file = os.path.join(temp_dir, f"temp_{len(temp_files)}.csv")
                df.to_csv(temp_file, index=False)
                temp_files.append(temp_file)
                
                # 清理内存
                del df
                gc.collect()
                
            except Exception as e:
                print(f"处理文件 {file_path} 时出错: {str(e)}")
                continue
    
    # 合并临时文件，分批处理
    chunk_size = 10  # 可根据内存大小调整
    final_temp_files = []
    
    for i in tqdm(range(0, len(temp_files), chunk_size), desc="合并临时文件"):
        chunk = temp_files[i:i+chunk_size]
        dfs = []
        
        for f in chunk:
            try:
                df = pd.read_csv(
                    f,
                    parse_dates=['date'],
                )
                dfs.append(df)
            except Exception as e:
                print(f"读取临时文件 {f} 时出错: {str(e)}")
                continue
        
        if dfs:  # 只有当有数据时才合并
            combined = pd.concat(dfs, ignore_index=True)
            # 提前排序以减少最终排序的内存压力
            combined = combined.sort_values(by=['serial_number', 'date'])
            
            # 保存合并后的结果
            final_temp = os.path.join(temp_dir, f"final_temp_{i//chunk_size}.csv")
            combined.to_csv(final_temp, index=False)
            final_temp_files.append(final_temp)
            
            # 清理内存
            del dfs, combined
            gc.collect()
    
    # 合并最后剩下的临时文件
    dfs = []
    for f in tqdm(final_temp_files, desc="最终合并"):
        try:
            df = pd.read_csv(
                f,
                parse_dates=['date'],
            )
            dfs.append(df)
        except Exception as e:
            print(f"读取最终临时文件 {f} 时出错: {str(e)}")
            continue
    
    # 最终合并和排序
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        sorted_df = combined_df.sort_values(by=['serial_number', 'date'])
        
        # 确保输出目录存在
        output_dir = "./data/"
        os.makedirs(output_dir, exist_ok=True)
        
        # 保存最终结果
        output_path = os.path.join(output_dir, "2017_combined_sorted.csv")
        sorted_df.to_csv(output_path, index=False)
        print(f"合并和排序完成，结果已保存到 {output_path}")
        
        # 最后清理
        del dfs, combined_df, sorted_df
        gc.collect()
    else:
        print("没有有效的数据进行合并和排序")



