"""
main module of mondrian_l_diversity
"""
#!/usr/bin/env python
# coding=utf-8

# @InProceedings{LeFevre2006a,
#   Title = {Workload-aware Anonymization},
#   Author = {LeFevre, Kristen and DeWitt, David J. and Ramakrishnan, Raghu},
#   Booktitle = {Proceedings of the 12th ACM SIGKDD International Conference on Knowledge Discovery and Data Mining},
#   Year = {2006},
#   Address = {New York, NY, USA},
#   Pages = {277--286},
#   Publisher = {ACM},
#   Series = {KDD '06},
#   Acmid = {1150435},
#   Doi = {10.1145/1150402.1150435},
#   ISBN = {1-59593-339-5},
#   Keywords = {anonymity, data recoding, predictive modeling, privacy},
#   Location = {Philadelphia, PA, USA},
#   Numpages = {10},
#   Url  = {http://doi.acm.org/10.1145/1150402.1150435}
# }

# 2014-10-12

import pdb
import time
from models.numrange import NumRange
from models.gentree import GenTree
from utils.utility import list_to_str, cmp_str


__DEBUG = False
QI_LEN = 10
GL_L = 5
RESULT = []
ATT_TREES = []
QI_RANGE = []
IS_CAT = []


class Partition(object):

    """Class for Group, which is used to keep records
    Store tree node in instances.
    self.member: records in group
    self.width: width of this partition on each domain
    self.middle: save the generalization result of this partition
    self.allow: 0 donate that not allow to split, 1 donate can be split
    """

    def __init__(self, data, width, middle):
        """
        initialize with data, width and middle
        """
        self.member = data[:]
        self.width = list(width)
        self.middle = list(middle)
        self.allow = [1] * QI_LEN

    def add_record(self, record):
        """
        add record to partition
        """
        self.member.append(record)

    def __len__(self):
        """
        return the number of records in partition
        """
        return len(self.member)


def check_diversity(data):
    """
    check the distinct SA values in dataset
    """
    sa_dict = {}
    for record in data:
        try:
            sa_value = list_to_str(record[-1])
        except AttributeError:
            sa_value = record[-1]
        try:
            sa_dict[sa_value] += 1
        except KeyError:
            sa_dict[sa_value] = 1
    return len(sa_dict.keys())


def check_L_diversity(partition):
    """check if partition satisfy l-diversity
    return True if satisfy, False if not.
    """
    sa_dict = {}
    if len(partition) < GL_L:
        return False
    if isinstance(partition, Partition):
        records_set = partition.member
    else:
        records_set = partition
    num_record = len(records_set)
    for record in records_set:
        try:
            sa_value = list_to_str(record[-1])
        except AttributeError:
            sa_value = record[-1]
        try:
            sa_dict[sa_value] += 1
        except KeyError:
            sa_dict[sa_value] = 1
    if len(sa_dict.keys()) < GL_L:
        return False
    for sa_value in sa_dict.keys():
        # if any SA value appear more than |T|/l,
        # the partition does not satisfy l-diversity
        if sa_dict[sa_value] > 1.0 * num_record / GL_L:
            return False
    return True


def get_normalized_width(partition, index):
    """
    return Normalized width of partition
    similar to NCP
    """
    if IS_CAT[index] is False:
        low = partition.width[index][0]
        high = partition.width[index][1]
        width = float(ATT_TREES[index].sort_value[high]) - float(ATT_TREES[index].sort_value[low])
    else:
        width = partition.width[index]
    return width * 1.0 / QI_RANGE[index]


def choose_dimension(partition):
    """chooss dim with largest normlized Width
    return dim index.
    """
    max_witdh = -1
    max_dim = -1
    for i in range(QI_LEN):
        if partition.allow[i] == 0:
            continue
        norm_width = get_normalized_width(partition, i)
        if norm_width > max_witdh:
            max_witdh = norm_width
            max_dim = i
    if max_witdh > 1:
        print "Error: max_witdh > 1"
        pdb.set_trace()
    if max_dim == -1:
        print "cannot find the max dim"
        pdb.set_trace()
    return max_dim


def frequency_set(partition, dim):
    """get the frequency_set of partition on dim
    return dict{key: str values, values: count}
    """
    frequency = {}
    for record in partition.member:
        try:
            frequency[record[dim]] += 1
        except KeyError:
            frequency[record[dim]] = 1
    return frequency


def find_median(partition, dim):
    """find the middle of the partition
    return splitVal
    """
    frequency = frequency_set(partition, dim)
    splitVal = ''
    nextVal = ''
    value_list = frequency.keys()
    value_list.sort(cmp=cmp_str)
    total = sum(frequency.values())
    middle = total / 2
    if middle < GL_L:
        return '', ''
    index = 0
    split_index = 0
    for i, qid_value in enumerate(value_list):
        index += frequency[qid_value]
        if index >= middle:
            splitVal = qid_value
            split_index = i
            break
    else:
        print "Error: cannot find splitVal"
    try:
        nextVal = value_list[split_index + 1]
    except IndexError:
        nextVal = splitVal
    return (splitVal, nextVal)


def split_numeric_value(numeric_value, splitVal, nextVal):
    """
    split numeric value on splitVal
    return sub ranges
    """
    split_result = numeric_value.split(',')
    if len(split_result) <= 1:
        return split_result[0], split_result[0]
    else:
        low = split_result[0]
        high = split_result[1]
        # Fix 2,2 problem
        if low == splitVal:
            lvalue = low
        else:
            lvalue = low + ',' + splitVal
        if high == splitVal:
            rvalue = high
        else:
            rvalue = nextVal + ',' + high
        return lvalue, rvalue


def anonymize(partition):
    """
    Main procedure of mondrian_l_diversity.
    recursively partition groups until not allowable.
    """
    allow_count = sum(partition.allow)
    pwidth = partition.width
    pmiddle = partition.middle
    for index in range(allow_count):
        dim = choose_dimension(partition)
        if dim == -1:
            print "Error: dim=-1"
            pdb.set_trace()
        if IS_CAT[dim] is False:
            # numeric attributes
            (splitVal, nextVal) = find_median(partition, dim)
            if splitVal == '':
                partition.allow[dim] = 0
                continue
            middle_pos = ATT_TREES[dim].dict[splitVal]
            lhs_middle = pmiddle[:]
            rhs_middle = pmiddle[:]
            lhs_middle[dim], rhs_middle[dim] = split_numeric_value(pmiddle[dim], splitVal, nextVal)
            lhs_width = pwidth[:]
            rhs_width = pwidth[:]
            lhs_width[dim] = (pwidth[dim][0], middle_pos)
            rhs_width[dim] = (ATT_TREES[dim].dict[nextVal], pwidth[dim][1])
            lhs = []
            rhs = []
            for record in partition.member:
                pos = ATT_TREES[dim].dict[record[dim]]
                if pos <= middle_pos:
                    # lhs = [low, means]
                    lhs.append(record)
                else:
                    # rhs = (means, high]
                    rhs.append(record)
            if check_L_diversity(lhs) is False or check_L_diversity(rhs) is False:
                partition.allow[dim] = 0
                continue
            # anonymize sub-partition
            anonymize(Partition(lhs, lhs_width, lhs_middle))
            anonymize(Partition(rhs, rhs_width, rhs_middle))
            return
        else:
            # normal attributes
            split_node = ATT_TREES[dim][partition.middle[dim]]
            if len(split_node.child) == 0:
                partition.allow[dim] = 0
                continue
            sub_node = [t for t in split_node.child]
            sub_partitions = []
            for i in range(len(sub_node)):
                sub_partitions.append([])
            for record in partition.member:
                qid_value = record[dim]
                for i, node in enumerate(sub_node):
                    try:
                        node.cover[qid_value]
                        sub_partitions[i].append(record)
                        break
                    except KeyError:
                        continue
                else:
                    print "Generalization hierarchy error!"
                    pdb.set_trace()
            flag = True
            for sub_partition in sub_partitions:
                if len(sub_partition) == 0:
                    continue
                if check_L_diversity(sub_partition) is False:
                    flag = False
                    break
            if flag:
                for i, sub_partition in enumerate(sub_partitions):
                    if len(sub_partition) == 0:
                        continue
                    wtemp = pwidth[:]
                    mtemp = pmiddle[:]
                    wtemp[dim] = len(sub_node[i])
                    mtemp[dim] = sub_node[i].value
                    anonymize(Partition(sub_partition, wtemp, mtemp))
                return
            else:
                partition.allow[dim] = 0
                continue
    RESULT.append(partition)


def init(att_trees, data, L, QI_num=-1):
    """
    resset global variables
    """
    global GL_L, RESULT, QI_LEN, ATT_TREES, QI_RANGE, IS_CAT
    ATT_TREES = att_trees
    if QI_num <= 0:
        QI_LEN = len(data[0]) - 1
    else:
        QI_LEN = QI_num
    for gen_tree in att_trees:
        if isinstance(gen_tree, NumRange):
            IS_CAT.append(False)
        else:
            IS_CAT.append(True)
    GL_L = L
    RESULT = []
    QI_RANGE = []


def mondrian_l_diversity(att_trees, data, l, QI_num=-1):
    """
    Mondrian for l-diversity.
    This fuction support both numeric values and categoric values.
    For numeric values, each iterator is a mean split.
    For categoric values, each iterator is a split on GH.
    The final result is returned in 2-dimensional list.
    """
    init(att_trees, data, l, QI_num)
    middle = []
    result = []
    wtemp = []
    for i in range(QI_LEN):
        if IS_CAT[i] is False:
            QI_RANGE.append(ATT_TREES[i].range)
            wtemp.append((0, len(ATT_TREES[i].sort_value) - 1))
            middle.append(ATT_TREES[i].value)
        else:
            QI_RANGE.append(len(ATT_TREES[i]['*']))
            wtemp.append(len(ATT_TREES[i]['*']))
            middle.append('*')
    whole_partition = Partition(data, wtemp, middle)
    start_time = time.time()
    anonymize(whole_partition)
    rtime = float(time.time() - start_time)
    ncp = 0.0
    dp = 0.0
    for partition in RESULT:
        rncp = 0.0
        dp += len(partition) ** 2
        for index in range(QI_LEN):
            rncp += get_normalized_width(partition, index)
        for index in range(len(partition)):
            gen_result = partition.middle + [partition.member[index][-1]]
            result.append(gen_result[:])
        rncp *= len(partition)
        ncp += rncp
    ncp /= QI_LEN
    ncp /= len(data)
    ncp *= 100
    if __DEBUG:
        print "L=%d" % l
        from decimal import Decimal
        print "Discernability Penalty=%.2E" % Decimal(str(dp))
        print "Diversity", check_diversity(data)
        # If the number of raw data is not eual to number published data
        # there must be some problems.
        print "size of partitions", len(RESULT)
        print "Number of Raw Data", len(data)
        print "Number of Published Data", sum([len(t) for t in RESULT])
        # print [len(t) for t in RESULT]
        print "NCP = %.2f %%" % ncp
    if len(result) != len(data):
        print "Error: lose records"
        pdb.set_trace()
    return (result, (ncp, rtime))
