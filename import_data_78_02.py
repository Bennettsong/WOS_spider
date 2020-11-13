'''
Descripttion: 
version: 
Author: Bennett
Date: 2019-04-13 21:51:05
LastEditTime: 2020-11-13 16:59:16
'''
import pymysql
import os

# 打开数据库连接
db = pymysql.connect("localhost", "username", "password", "databaseName")
# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor(cursor=pymysql.cursors.DictCursor)

def import_data(directory):
    path = os.listdir(directory)
    # 每个文件存储一次
    for files in path:
        file_path = directory + files
        # 数据库需要的数据
        sql_data = []
        # 去重字段
        UT = []
        with open(file_path, 'r', encoding='UTF-8') as file:
            for index,line in enumerate(file.readlines()):
                if index == 0:
                    continue
                tmp = line.strip()
                if not tmp:
                    continue
                tmp = tmp.split('\t')
                if tmp[61] not in UT:
                    sql_data.append(tmp)
                    UT.append(tmp[61])
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
                print("文件：" + files + " 导入成功")
            elif row_1['@_insert_data_1'] == 500:
                print("文件：" + files + " 导入失败")
            # 执行sql语句
            # 当row_1为200表示成功，500表示失败
            # 提交到数据库执行
            db.commit()
        except:
            # 如果发生错误则回滚
            db.rollback()
            
    # 关闭数据库连接
    cursor.close()
    db.close()
directory = '文件夹路径'
import_data(directory)

