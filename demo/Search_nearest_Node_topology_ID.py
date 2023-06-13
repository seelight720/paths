# -*- coding:utf-8 -*-
import networkx as nx
import xlrd
import matplotlib.pyplot as plt
import datetime as DT
import sys
import multiprocessing
import math
from math import radians


def load_Node_topology(filename):
    print('读取Node_topology')
    ######加载表格中信息并把没边的点删除##########
    data_1 = xlrd.open_workbook(filename)
    sheet_1 = data_1.sheet_by_index(0)

    # 读取节点集合
    Node_topology_dict = {}
    for i in range(sheet_1.nrows):
        row = sheet_1.row_values(i)
        Node_topology_dict[int(row[0])] = [float(row[1]), float(row[2])]

    return Node_topology_dict


def load_Unmatched_node(filename,ProcessNumb):
    print('读取Unmatched_node')
    Unmatched_node_list = []
    with open(filename, "r",encoding="UTF-8-sig") as f:
        i=0
        for line in f:
            if i>0:
                line = line.strip('\n')  # 去掉列表中每一个元素的换行符
                arrline = line.split(',')
                Unmatched_node_list.append([ int(arrline[1]), float(arrline[9]), float(arrline[10]) ])
            i=i+1

    Unmatched_node_dict={}
    count=int(len(Unmatched_node_list)/ProcessNumb)
    if count<1:
        Unmatched_node_dict[0]=Unmatched_node_list[1:]
        print()
    else:
        for i in range(ProcessNumb):
            if i==ProcessNumb-1:
                Unmatched_node_dict[i]=Unmatched_node_list[i*count :]
            else:
                Unmatched_node_dict[i]=Unmatched_node_list[i*count :(i+1)*count]

    print("数据共{0}条，划分为{1}份并行计算".format(len(Unmatched_node_list),len(Unmatched_node_dict)))
    for tempkey in Unmatched_node_dict.keys():
        print("第{0}份数据{1}条".format(tempkey,len(Unmatched_node_dict[tempkey])))
    return Unmatched_node_dict

# 查询最短路
def get_nearest_Node_topology_ID(Unmatched_node_ID_x,Unmatched_node_ID_y,Node_topology_dict):
    path_length="NAN"
    path=[]

    nearest_dist = 90000000000
    nearest_i_Node_topology_ID="-1"

    for i_Node_topology_dict in Node_topology_dict.keys():
        lon1 = Unmatched_node_ID_x
        lat1 = Unmatched_node_ID_y

        lonlat=Node_topology_dict[i_Node_topology_dict]
        lon2=lonlat[0]
        lat2=lonlat[1]

        if  lon1-0.2<lon2<lon1+0.2 and   lat1-0.2<lat2<lat1+0.2:
            lon1, lat1, lon2, lat2 = map(radians, map(float, [lon1, lat1, lon2, lat2]))
            dlon = lon2 - lon1
            dlat = lat2 - lat1
            a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
            c = 2 * math.asin(math.sqrt(a))
            r = 6371
            ret = c * r
            # result = "%.7f" % ret
            if ret< nearest_dist:
                nearest_dist = ret
                nearest_i_Node_topology_ID = i_Node_topology_dict

    return nearest_i_Node_topology_ID

def start_nearest_Node( Unmatched_node_dict, Node_topology_dict ):
    Unmatched_node_topology_ID=[]
    for key in Unmatched_node_dict.keys():
        key_process=Unmatched_node_dict[key]
        for j in range(len(key_process)):
            nearest_Node_topology_ID=get_nearest_Node_topology_ID( key_process[j][1],key_process[j][2],Node_topology_dict)
            Unmatched_node_topology_ID.append([key_process[j][0],nearest_Node_topology_ID ])
            if j%1000==0:
                print(str(j))
         # 保存结果
        with open(r'../data/Unmatched_node_topology_ID'+str(key)+'.txt', "a+") as f:
            for i in range(len(Unmatched_node_topology_ID)):
                f.write('{0},{1}{2}'.format(Unmatched_node_topology_ID[i][0] , Unmatched_node_topology_ID[i][1],'\n'))

if __name__=='__main__':
    ProcessNumb=8 #进程数量

    Unmatched_node_dict=load_Unmatched_node(r'../data/numatched_node_huzhou.txt',ProcessNumb)#相同进程要处理的数据放在同一个key中
    Node_topology_dict=load_Node_topology(r'../data/node_huzhou.xlsx')

    print(DT.datetime.now())#开始时间
    start_nearest_Node(Unmatched_node_dict,Node_topology_dict )
    
    # for i in range(1000):
    #     get_nearest_Node_topology_ID( 119.543538, 28.96488,Node_topology_dict)

    # process_pool(G,node,OD_dict)

    print(DT.datetime.now())#结束时间
    sys.exit()
