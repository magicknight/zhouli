from random import shuffle, choice
from itertools import combinations
from math import sqrt
from pprint import pprint

n_run = 10000000


def di_average(n=10):
    data = list(range(1, n+1))
    shuffle(data)
    total = 0.0
    total_comb = 0
    for each_item in combinations(data, 2):
        total += abs(each_item[0] - each_item[1])
        total_comb += 1
    first_average = sum(list(range(1, n+1)))/n
    return (first_average + total/total_comb*(n-1))/n


def di_deviation(n=10):
    data = list(range(1, n+1))
    shuffle(data)
    total = 0.0
    total_comb = 0
    for each_item in combinations(data, 2):
        total += pow(each_item[0] - each_item[1], 2)
        total_comb += 1
    first_average = sum(list(range(1, n+1)))/n
    return (first_average + total/total_comb*(n-1))/n

if __name__ == '__main__':
    average_10 = di_average(10)
    print('%.10f' % average_10)
    average_20 = di_average(20)
    print('%.10f' % average_20)
    deviation_10 = di_deviation(10)
    deviation_10 = sqrt(deviation_10 - pow(average_10, 2))
    print('%.10f' % deviation_10)
    deviation_20 = di_deviation(20)
    deviation_20 = sqrt(deviation_20 - pow(average_20, 2))
    print('%.10f' % deviation_20)

