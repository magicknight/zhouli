"""
process data
"""
from pprint import pprint
from time import time
import numpy as np
import arrow
import bottleneck as bn

keys = ['ip', 'resource', 'status']
# reference dict
reference = {'ip': {}, 'resource': {}, 'status': {}}
# inverse reference dict
inv_reference = {'ip': {}, 'resource': {}, 'status': {}}
data = dict.fromkeys(keys)


def rules(char):
    """
    rules for filter chars
    :param char: a char
    :return:
    """
    if char in ['-', '[', '“', '”', '"', ']']:
        return False
    else:
        return True


def convert_time(str_arr):
    """
    convert date_time to timestamp
    :param str_arr:
    :return:
    """
    date_time = np.array(list(map(lambda el: arrow.get(el, 'DD/MMM/YYYY:HH:mm:ss').timestamp, str_arr)))
    data['time'] = date_time


def feature_1(output):
    """
    feature 1
    :param output:
    :return:
    """
    count = np.bincount(data['ip'])
    most_numbers = np.partition(count, len(count)-10)[-10:]
    ips = []
    for each_number in most_numbers:
        temp = np.argwhere((count == each_number))
        for i in temp.flatten():
            ips.append(i)
    ips = np.array(list(set(ips)))
    n_ips = []
    for ip in ips:
        # times = np.sum(data['ip'] == ip)
        times = count[ip]
        n_ips.append(times)
    n_ips = np.array(n_ips)
    arg_sort = np.argsort(-n_ips)
    n_ips = n_ips[arg_sort]
    ips = ips[arg_sort]
    ips = ips[:10]
    with open(output, 'w') as f:
        for i in range(len(ips)):
            line = reference['ip'][str(ips[i])] + ', ' + str(n_ips[i]) + '\n'
            f.write(line)


def feature_2(output):
    """
    feature 2
    :param output:
    :return:
    """
    count = np.bincount(data['resource'], weights=data['size'])
    most_numbers = np.partition(count, len(count) - 10)[-10:]
    # pprint(most_numbers)
    resources = []
    for each_number in most_numbers:
        temp = np.argwhere((count == each_number))
        for i in temp.flatten():
            resources.append(i)
    resources = np.array(list(set(resources)))
    n_resources = []
    for resource in resources:
        times = count[resource]
        n_resources.append(times)
    n_resources = np.array(n_resources)
    arg_sort = np.argsort(-n_resources)
    n_resources = n_resources[arg_sort]
    resources = resources[arg_sort]
    resources = resources[:10]
    with open(output, 'w') as f:
        for i in range(len(resources)):
            line = reference['resource'][str(resources[i])] + '\n'
            f.write(line)


def feature_3(output):
    """
    feature 3
    :param output:
    :return:
    """
    start = np.amin(data['time'])
    times = data['time'] - start
    # pprint(times)
    counts = np.bincount(times)
    # pprint(counts)
    moving_sum = bn.move_sum(counts, window=60, axis=0)
    moving_sum = np.delete(moving_sum, range(0, 59))
    moving_sum = np.append(moving_sum, np.zeros(60))
    most_numbers = np.partition(moving_sum, len(moving_sum) - 10)[-10:]
    resources = []
    for each_number in most_numbers:
        temp = np.argwhere((moving_sum == each_number))
        for i in temp.flatten():
            resources.append(i)
    resources = np.array(list(set(resources)))
    n_resources = []
    for resource in resources:
        temp = moving_sum[resource]
        n_resources.append(temp)
    n_resources = np.array(n_resources)
    arg_sort = np.argsort(-n_resources)
    n_resources = n_resources[arg_sort]
    resources = resources[arg_sort]
    time_format = []
    for i in resources:
        date_time = arrow.get(i + start)
        time_format.append(date_time.format(fmt='DD/MMM/YYYY:HH:mm:ss'))

    time_format = time_format[:10]
    n_resources = n_resources[:10]

    with open(output, 'w') as f:
        for i in range(len(time_format)):
            line = time_format[i] + ' 0400, ' + str(n_resources[i]) + '\n'
            f.write(line)


def read_log(file):
    """

    :param file: input file path
    :return: list of: ip, date_time, resource, status, size
    """
    with open(file, encoding="ISO-8859-1") as f:
        result = []
        for line in f:
            line = ''.join(list(filter(rules, line)))
            row = line.split()
            if len(row) != 8:
                continue
            if row[0] not in inv_reference['ip']:
                inv_reference['ip'][row[0]] = len(inv_reference['ip'])
                reference['ip'][str(len(reference['ip']))] = row[0]
            if row[4] not in inv_reference['resource']:
                inv_reference['resource'][row[4]] = len(inv_reference['resource'])
                reference['resource'][str(len(reference['resource']))] = row[4]
            if row[6] not in inv_reference['status']:
                inv_reference['status'][row[6]] = len(inv_reference['status'])
                reference['status'][str(len(reference['status']))] = row[6]
            row[0] = inv_reference['ip'][row[0]]
            row[4] = inv_reference['resource'][row[4]]
            row[6] = inv_reference['status'][row[6]]
            result.append(row)
        result = np.array(result)
        # pprint(result.shape)
        date_time = result[:, 1]
        data['size'] = result[:, 7].astype(int)
        data['ip'] = result[:, 0].astype(int)
        data['resource'] = result[:, 4].astype(int)
        data['status'] = result[:, 6].astype(int)
        convert_time(date_time)


if __name__ == '__main__':
    print('running')

    start = time()
    input_file = './log_input/test10000.txt'
    print('reading log.txt, it may take several minutes, pls be patient....')
    read_log(input_file)
    end = time()
    print('time for read log:', end - start)
    # pprint(data)
    # pprint(reference)

    # solution 1
    start = time()
    feature_1('./log_output/hosts.txt')
    end = time()
    print('time for feature 1:', end - start)
    # solution 2
    start = time()
    feature_2('./log_output/resources.txt')
    end = time()
    print('time for feature 2:', end - start)
    # solution 3
    start = time()
    feature_3('./log_output/hours.txt')
    end = time()
    print('time for feature 3:', end - start)




