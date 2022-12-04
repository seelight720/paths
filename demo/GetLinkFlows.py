# -*- coding:utf-8 -*-

import networkx as nx
import xlrd
import matplotlib.pyplot as plt
import datetime as DT
import sys
import multiprocessing
import math
from math import radians


def loadOD_paths(filename):
    print('loadOD_paths: {0}'.format(filename))
    links_dict = {}
    with open(filename, "r",encoding="UTF-8-sig") as f:
        for line in f:
            line = line.strip('\n')  # 去掉列表中每一个元素的换行符
            arrline = line.split(',')
            temp_key=arrline[1]+","+arrline[3]+","+arrline[4]+","+arrline[5]+","+arrline[4]+"_"+arrline[5]
            temp_value=float(arrline[6])
            if temp_key in links_dict.keys():
                links_dict[temp_key] = links_dict[temp_key] +temp_value
            else:
                links_dict[temp_key]=temp_value
    return links_dict

def save_links_flows(links_dict):
    # 保存结果
    with open(r'../data/links_flows.txt', "a+") as f:
        for tempkey in links_dict.keys():
            f.write('{0},{1}{2}'.format(tempkey ,links_dict[tempkey],  '\n'))


if __name__ == '__main__':
    print(DT.datetime.now()  )  # 开始时间

    links_dict=loadOD_paths(r'../data/通勤/0.txt')
    save_links_flows(links_dict)

    print(DT.datetime.now()  )  # 结束时间
    sys.exit()
