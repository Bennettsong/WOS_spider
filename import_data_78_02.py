'''
Descripttion: 
version: 
Author: Bennett
Date: 2019-04-13 21:51:05
LastEditTime: 2020-11-14 16:22:10
'''
import pymysql
import os
from tqdm import tqdm

# 打开数据库连接
db = pymysql.connect("ip", "username", "password", "database")
# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor(cursor=pymysql.cursors.DictCursor)
success_import_file = []
fail_import_file = []


def callProc(sql_data, files):
    need_data = ""
    for item in sql_data:
        need_data = need_data + str(tuple(item)) + ","
    need_data = need_data[:len(need_data) - 1]

    # 导入数据库
    cursor.callproc('insert_data', args=(need_data, 1))
    try:
        cursor.execute("select @_insert_data_1")
        row_1 = cursor.fetchone()
        if row_1['@_insert_data_1'] == 200:
            tmp_print = "文件：" + files + " 导入成功"
            # print(tmp_print)
            success_import_file.append(tmp_print)
        elif row_1['@_insert_data_1'] == 500:
            tmp_print = "文件：" + files + " 导入失败"
            # print(tmp_print)
            fail_import_file.append(tmp_print)
        # 执行sql语句
        # 当row_1为200表示成功，500表示失败
        # 提交到数据库执行
        db.commit()
    except:
        # 如果发生错误则回滚
        db.rollback()


def import_data(directory):

    path = os.listdir(directory)
    # 每个文件存储一次
    for files in tqdm(path):
        try:
            file_path = directory + files
            # 数据库需要的数据
            sql_data = []
            # 去重字段
            UT = []

            with open(file_path, 'r', encoding='UTF-8') as file:
                for index, line in enumerate(file.readlines()):
                    if index == 0:
                        continue
                    tmp = line.strip()
                    if not tmp:
                        continue
                    tmp = tmp.split('\t')
                    if tmp[61] not in UT:
                        sql_data.append(tmp)
                        UT.append(tmp[61])

                    # 当单个文件过大的时候需要拆分
                    if index %200 ==0:
                        callProc(sql_data, files)
                        sql_data = []
            if sql_data:
                callProc(sql_data, files)
        except:
            with open('log.txt', 'a', encoding="utf-8") as tmp:
                # tmp.write("当前导入的文件")
                print("导入失败")
                tmp.write('\n')
                tmp.write(files)
                # tmp.write("导入成功的文件")
                # tmp.write('\n'.join(success_import_file))
                # tmp.write("导入失败的文件")
                # tmp.write('\n'.join(fail_import_file))
            # break

    # 关闭数据库连接
    cursor.close()
    db.close()


directory = '文件路径'
import_data(directory)
