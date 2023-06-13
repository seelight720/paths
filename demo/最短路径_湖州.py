# -*- coding:utf-8 -*-
import networkx as nx
import xlrd
import matplotlib.pyplot as plt
import datetime as DT
import sys
import multiprocessing

def my_graph():
    print('构建路网拓扑')
    ######加载表格中信息并把没边的点删除##########
    data_1 = xlrd.open_workbook(r'../data/node_huzhou.xlsx')
    sheet_1 = data_1.sheet_by_index(0)
    data_2 = xlrd.open_workbook(r'../data/edge_huzhou.xlsx')
    sheet_2 = data_2.sheet_by_index(0)

    # 读取节点集合
    node = {}
    for i in range(sheet_1.nrows):
        row = sheet_1.row_values(i)
        node[int(row[0])] = [float(row[1]), float(row[2])]

    G = nx.DiGraph(my_g='my_graph') # 有向图

    #加载路网节点
    print('加载路网：节点')
    # G = nx.Graph()
    for i in node:
        G.add_node(i,index=str(i))

    #加载路网边
    print('加载路网：边')
    edge={}
    for i in range(sheet_2.nrows):
        row = sheet_2.row_values(i)
        row_1 = int(row[0])
        row_2 = int(row[1])
        edge[str(row[0]).strip('.0') +','+str(row[1]).strip('.0')] = row[2]#各边距离字典
        if row_1 in node and row_2 in node:
            G.add_edge(row_1, row_2, weight=row[2])
    return G,node

 # 查询最短路
def get_shortest_path(G,node,START,END):
    # print('查询最短路：{0} to {1}'.format(START, END))
    path_length="NAN"
    path=[]
    if START in node:
        if END in node:
            try:
                path = nx.dijkstra_path(G, source=START, target=END,weight='weight')
                # print('路径：', path)
                # path_length = nx.dijkstra_path_length(G, source=START, target=END,weight='weight')
                # print('路径长度：', path_length)

                # 画最短路
                # path_edges = list(zip(path, path[1:]))
                # nx.draw_networkx_edges(G, node, edgelist=path_edges, edge_color='blue', width=2,node_size=0)
            except:
                path_length = "NAN"
        else:
            print("node {0} 不在路网中".format(END))
            path_length = "NAN"
    else:
        print("node {0} 不在路网中".format(START))
        path_length = "NAN"

    # nx.draw(G, node, node_size=1, with_labels=False)
    # plt.show()
    # return str(path_length)
    return path

def get_shortest_pathlength(G,node,START,END):
    # print('查询最短路：{0} to {1}'.format(START, END))
    path_length="NAN"
    path=[]
    if START in node:
        if END in node:
            try:
                # path = nx.dijkstra_path(G, source=START, target=END,weight='weight')
                # print('路径：', path)
                path_length = nx.dijkstra_path_length(G, source=START, target=END,weight='weight')
                # print('路径长度：', path_length)

                # 画最短路
                # path_edges = list(zip(path, path[1:]))
                # nx.draw_networkx_edges(G, node, edgelist=path_edges, edge_color='blue', width=2,node_size=0)
            except:
                path_length = "NAN"
        else:
            # print("node {0} 不在路网中".format(END))
            path_length = "NAN"
    else:
        # print("node {0} 不在路网中".format(START))
        path_length = "NAN"

    # nx.draw(G, node, node_size=1, with_labels=False)
    # plt.show()
    return str(path_length)
    # return path

def process_pool(G,Unmatched_node_dict,node,OD_dict):
    ProcessNumb=len(OD_dict)
    # 在进程池中准备 ProcessNumb个进程
    pool = multiprocessing.Pool(ProcessNumb)
    # 共有 4 个任务要执行
    # ProcessNumb 个进程要去执行 4 个任务，进程数是不够的
    # 进程池的机制为，当一个进程执行完任务后将重新回到进程池中备用
    # 如果还有任务要执行，那么就从进程池中拿出空闲的进程使用
    for i in range(ProcessNumb):
        # pool.apply_async(start_proecss_4_mutiOD, args=(G,Unmatched_node_dict,node,OD_dict[i] ,i))
        pool.apply_async(start_proecss_3_mutiOD, args=(G, Unmatched_node_dict, node, OD_dict[i], i))
    # 先关闭进程池，意思为进程池不再接受新的任务
    pool.close()
    # 将进程加入到主进程中，防止子进程尚未结束，主进程已经执行完，导致杀死子进程。
    # 如果没有 pool.join()，那么主进程在执行完 pool.close() 后其代码结束，所以主进程会关闭。
    # 而加入 pool.join() 意味着子进程的代码也算在主进程代码内，子进程没完，则主进程也没完
    # 此时主进程会等待子进程结束后再结束。
    pool.join()

def start_proecss_4_mutiOD(G,Unmatched_node_dict,node,OD_list,ProcessNumb_id):
    result_list=[]
    result_dict={}
    for i in range(len(OD_list)):
        path=get_shortest_path(G, node, Unmatched_node_dict[OD_list[i][2]], Unmatched_node_dict[OD_list[i][3]])
        if len(path)>1:
            links_path=list(zip(path,path[1:]))
            for i_links_path in range(len(links_path)):
                result_dict[str(OD_list[i][0])+","+ str(OD_list[i][1])+","+str(OD_list[i][2])+","+str(OD_list[i][3])+","+str(links_path[i_links_path][0])+","+str(links_path[i_links_path][1])]=OD_list[i][4]
        if i%10000==0:
            print("Time:{2},Process {0} 已计算完成{1}条".format(ProcessNumb_id,i,DT.datetime.now()))

     # 保存结果
    with open(r'../data/'+str(ProcessNumb_id)+'.txt', "a+") as f:
        for j_tempkey in result_dict.keys():
            # f.writelines(j_tempkey+","+result_dict[j_tempkey]+"\n")
            f.write('{0},{1}{2}'.format(j_tempkey , result_dict[j_tempkey],'\n'))



def start_proecss_3_mutiOD(G,Unmatched_node_dict,node,OD_list,ProcessNumb_id):
    result_list=[]
    result_dict={}
    for i in range(len(OD_list)):
        # path=get_shortest_path(G, node, Unmatched_node_dict[OD_list[i][0]], Unmatched_node_dict[OD_list[i][2]])
        path_length = get_shortest_pathlength(G,node, Unmatched_node_dict[OD_list[i][2]], Unmatched_node_dict[OD_list[i][3]])
        result_dict[str(OD_list[i][0]) + "," + str(OD_list[i][1]) + "," + str(OD_list[i][2]) + "," + str(OD_list[i][3]) + "," + str(OD_list[i][4])] = path_length

        # if len(path)>1:
        #     links_path=list(zip(path,path[1:]))
        #     for i_links_path in range(len(links_path)):
        #         result_dict[str(OD_list[i][0])+","+ str(OD_list[i][1])+","+str(OD_list[i][2])+","+str(OD_list[i][3])+","+ str(OD_list[i][4])   ]=OD_list[i][4]
        if i%10000==0:
            print("Time:{2},Process {0} 已计算完成{1}条".format(ProcessNumb_id,i,DT.datetime.now()))

     # 保存结果
    with open(r'../data/'+str(ProcessNumb_id)+'.txt', "a+") as f:
        for j_tempkey in result_dict.keys():
            # f.writelines(j_tempkey+","+result_dict[j_tempkey]+"\n")
            f.write('{0},{1}{2}'.format(j_tempkey , result_dict[j_tempkey],'\n'))

def load_Unmatched_node_topology_ID(filename):
    print('load_Unmatched_node_topology_ID: {0}'.format(filename))
    #文件格式TAZID, nodeID
    Unmatched_node_dict = {}
    with open(filename, "r",encoding="UTF-8-sig") as f:
        for line in f:
            line = line.strip('\n')  # 去掉列表中每一个元素的换行符
            arrline = line.split(',')
            Unmatched_node_dict[int(arrline[0])]=int(arrline[1])
    return Unmatched_node_dict

def loadOD(filename,ProcessNumb):
    print('loadOD: {0}'.format(filename))
    OD_list = []
    with open(filename, "r",encoding="UTF-8-sig") as f:
        for line in f:
            line = line.strip('\n')  # 去掉列表中每一个元素的换行符
            arrline = line.split(',')
            # OD_list.append([ int(arrline[2]), int(arrline[0]), int(arrline[3]),int(arrline[1]),float(arrline[4]) ])
            OD_list.append([str(arrline[0]), str(arrline[1]), int(arrline[2]), int(arrline[3]), float(arrline[4])])

    OD_dict={}
    count=int(len(OD_list)/ProcessNumb)
    if count<1:
        OD_dict[0]=OD_list[:]
    else:
        for i in range(ProcessNumb):
            if i==ProcessNumb-1:
                OD_dict[i]=OD_list[i*count :]
            else:
                OD_dict[i]=OD_list[i*count :(i+1)*count]

    print("数据共{0}条，划分为{1}份并行计算".format(len(OD_list),len(OD_dict)))
    for tempkey in OD_dict.keys():
        print("第{0}份数据{1}条".format(tempkey,len(OD_dict[tempkey])))
    return OD_dict

if __name__=='__main__':
    ProcessNumb=4 #进程数量

    Unmatched_node_dict=load_Unmatched_node_topology_ID(r'../data/Unmatched_node_topology_ID_湖州.txt')
    OD_dict=loadOD(r'../data/2交通小区间的通勤OD分布_huzhou.txt',ProcessNumb)#相同进程要处理的数据放在同一个key中
    G,node=my_graph()

    # paths2s=get_shortest_path(G,node,56336,56486)
    # start_proecss_3_mutiOD(G, Unmatched_node_dict, node, OD_dict[0], 1)

    print(DT.datetime.now())#开始时间

    process_pool(G,Unmatched_node_dict,node,OD_dict)

    print(DT.datetime.now())#结束时间
    sys.exit()
