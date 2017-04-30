#!/usr/bin/python3
from random import shuffle, choice
from itertools import combinations, permutations
from math import sqrt
from scipy.stats import norm
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
    return (pow(first_average, 2) + total/total_comb*(n-1))/n


def di_probability(avg, div, num):
    return norm(avg, div).cdf(num)


if __name__ == '__main__':
    average_10 = di_average(10)
    total_10 = average_10*10
    print('%.10f' % total_10)

    average_20 = di_average(20)
    total_20 = average_20 * 20
    print('%.10f' % total_20)

    deviation_10 = di_deviation(10)
    deviation_10 = sqrt(deviation_10 - pow(average_10, 2))
    deviation_10 = deviation_10 * sqrt(10)
    print('%.10f' % deviation_10)

    deviation_20 = di_deviation(20)
    deviation_20 = sqrt(deviation_20 - pow(average_20, 2))
    deviation_20 = deviation_20 * sqrt(20)
    print('%.10f' % deviation_20)

    probability_10_45 = 1 - di_probability(total_10, deviation_10, 45)
    print('%.10f' % probability_10_45)

    probability_20_160 = 1 - di_probability(total_20, deviation_20, 160)
    print('%.10f' % probability_20_160)

