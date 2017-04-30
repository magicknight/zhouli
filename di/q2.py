#!/usr/bin/python3
import csv
from scipy.stats import pearsonr, ttest_ind, ttest_rel
from math import log
import numpy as np
from pprint import pprint

data_path = '/home/zhihua/work/zhouli/data/CollegeScorecard_Raw_Data/'
key_list = ['SAT_AVG', 'UGDS', 'ENRL_ORIG_YR2_RT', 'LO_INC_COMP_ORIG_YR4_RT', 'HI_INC_COMP_ORIG_YR4_RT', 'UGDS_WOMEN']
key_race = [ 'UGDS_WHITE', 'UGDS_BLACK', 'UGDS_HISP', 'UGDS_ASIAN', 'UGDS_AIAN', 'UGDS_NHPI', 'UGDS_2MOR', 'UGDS_NRA', 'UGDS_UNKN', 'UGDS_WHITENH', 'UGDS_BLACKNH',
             'UGDS_API', 'UGDS_AIANOLD', 'UGDS_HISPOLD']
ten_year_files = ['MERGED2001_02_PP.csv', 'MERGED2002_03_PP.csv', 'MERGED2003_04_PP.csv', 'MERGED2004_05_PP.csv', 'MERGED2005_06_PP.csv',
                  'MERGED2006_07_PP.csv', 'MERGED2007_08_PP.csv', 'MERGED2008_09_PP.csv', 'MERGED2009_10_PP.csv', 'MERGED2010_11_PP.csv', ]
void_label = ['NULL', 'PrivacySuppressed', '0']


def read_in_dict(path):
    in_data = {}
    with open(path, 'r') as f:
        raw_data = list(csv.DictReader(f))
    for each_school in raw_data:
        in_data[each_school['OPEID']] = each_school
    return in_data


def read_in(path):
    with open(path, 'r') as f:
        raw_data = list(csv.DictReader(f))
    return raw_data


def average_sat(input_data):
    total_score = 0
    total_student = 0
    for each_school in input_data:
        if each_school['SAT_AVG'] == 'NULL' or each_school['UGDS'] == 'NULL':
            continue
        total_score += float(each_school['SAT_AVG'])*int(each_school['UGDS'])
        total_student += int(each_school['UGDS'])
    return total_score/total_student


def pearson_correlation(input_data):
    sat_score = []
    two_year_percentage = []
    for each_school in input_data:
        if each_school['SAT_AVG'] in void_label or each_school['ENRL_ORIG_YR2_RT'] in void_label:
            continue
        sat_score.append(float(each_school['SAT_AVG']))
        two_year_percentage.append(float(each_school['ENRL_ORIG_YR2_RT']))
    return pearsonr(sat_score, two_year_percentage)


def difference(input_data):
    high_income = 0
    low_income = 0
    n_schools = 0
    for each_school in input_data:
        if each_school['LO_INC_COMP_ORIG_YR4_RT'] in void_label or each_school['HI_INC_COMP_ORIG_YR4_RT'] in void_label:
            continue
        high_income += float(each_school['HI_INC_COMP_ORIG_YR4_RT'])
        low_income += float(each_school['LO_INC_COMP_ORIG_YR4_RT'])
        n_schools += 1
    return (high_income - low_income)/n_schools


def t_test(input_data, diff):
    high_income = []
    low_income = []
    for each_school in input_data:
        if each_school['LO_INC_COMP_ORIG_YR4_RT'] in void_label or each_school['HI_INC_COMP_ORIG_YR4_RT'] in void_label:
            continue
        high_income.append(float(each_school['HI_INC_COMP_ORIG_YR4_RT']))
        low_income.append(float(each_school['LO_INC_COMP_ORIG_YR4_RT']))

    # low_income = [i + diff for i in low_income]
    return ttest_ind(high_income, low_income)


def diverse(input_data):
    diverse_list = []
    for each_school in input_data:
        school_race = []
        for each_key in key_race:
            if each_school[each_key] in void_label:
                continue
            school_race.append(float(each_school[each_key]))
        if len(school_race) < 3:
            continue
        if school_race:
            amin = np.amin(school_race)
            amax = np.amax(school_race)
            diverse_list.append(amax - amin)
    return np.amin(diverse_list)


def women():
    ten_year_data = {}
    trash_ids = []
    average_women = 0
    n_schools = 0
    for each_year in ten_year_files:
        year_data = read_in_dict(data_path + each_year)
        for each_key in year_data.keys():
            if each_key in trash_ids:
                continue
            if year_data[each_key]['UGDS_WOMEN'] in void_label:
                trash_ids.append(each_key)
                if each_key in ten_year_data:
                    ten_year_data.pop(each_key, None)
                continue
            ten_year_data[each_key] = ten_year_data.setdefault(each_key, 0.0) + float(year_data[each_key]['UGDS_WOMEN'])
    for each_value in ten_year_data.values():
        average_women += each_value
        n_schools += 1
    average_women = average_women / n_schools / 10
    return average_women


if __name__ == '__main__':
    data = read_in(data_path + 'MERGED2013_14_PP.csv')
    sat = average_sat(data)
    print('%.10f' % sat)

    pearson_corr = pearson_correlation(data)
    print(pearson_corr)

    high_low_diff = difference(data)
    print(high_low_diff)

    p_value = t_test(data, high_low_diff)
    print(p_value)
    print(log(p_value[1]))

    diverse_min = diverse(data)
    print('%.10f' % diverse_min)

    average_women_share = women()
    print(average_women_share)








