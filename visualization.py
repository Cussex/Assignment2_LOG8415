
import numpy as np
import matplotlib.pyplot as plt

class Dataset:

    def __init__(self, name, real, user, sys ):
        self.name = name
        self.real = real
        self.user = user
        self.sys = sys

def read_linux_hadoop_file(filename):
    dataset_names =[]
    dataset_real_results = []
    file = open(filename, 'r')
    for line in file:
        name = line
        real = float (file.readline().split()[1])
        file.readline().split()[1]
        file.readline().split()[1]
        dataset_names = [name]
        dataset_real_results = [real]
    return dataset_names, dataset_real_results

def read_hadoop_spark_file(filename):
    file = open(filename, 'r')
    results_list = []
    for line in file:
        name = line
        real = float (file.readline().split()[1])
        user = float (file.readline().split()[1])
        sys = float (file.readline().split()[1]) 
        new_Dataset = Dataset(name, real, user, sys)
        results_list.append(new_Dataset)
    
    results_Ave_dic = {}
    for datasetObj in results_list:
        if datasetObj.name in results_Ave_dic:
            results_Ave_dic[datasetObj.name] = results_Ave_dic[datasetObj.name] + datasetObj.real
        else:
            results_Ave_dic[datasetObj.name] = datasetObj.real
        
    for key in results_Ave_dic.keys():
        results_Ave_dic[key] = results_Ave_dic[key]/3.0

    dataset_names =[]
    dataset_real_results = []
    for key in results_Ave_dic.keys():
        dataset_names.append(key)
        dataset_real_results.append(results_Ave_dic[key])

    return dataset_names, dataset_real_results

def show_plot(dataset_names1, dataset_real_results1 , dataset_real_results2, legend_name1, legend_name2):
    width = 0.4
    x = np.arange(len(dataset_names1))
    plt.bar(x-0.2, dataset_real_results1, width)
    plt.bar(x+0.2, dataset_real_results2, width)
    plt.xticks(x, dataset_names1, rotation=90)
    plt.ylabel("seconds")
    plt.legend([legend_name1 , legend_name2])
    plt.savefig("visualization/"+legend_name1+"_"+legend_name2+".png")
    plt.clf()

def main() :
    filename1 = 'results_hadoop_linux/hadoop_wordcount_time.txt'
    dataset_names1, dataset_real_results1=  read_linux_hadoop_file(filename1)

    filename2 = 'results_hadoop_linux/linux_wordcount_time.txt'
    _, dataset_real_results2 =  read_linux_hadoop_file(filename2)

    show_plot(dataset_names1, dataset_real_results1 , dataset_real_results2, "Hadoop" , "Linux")

    filename1 = 'results_hadoop_spark/hadoop_wordcount_time.txt'
    dataset_names1, dataset_real_results1=  read_hadoop_spark_file(filename1)

    filename2 = 'results_hadoop_spark/spark_wordcount_time.txt'
    _, dataset_real_results2 =  read_hadoop_spark_file(filename2)

    show_plot(dataset_names1, dataset_real_results1 , dataset_real_results2, "Hadoop" , "Spark")
