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
from models.numrange import NumRange
from models.gentree import GenTree
from utils.utility import list_to_str, cmp_str


__DEBUG = True
QI_LEN = 10
GL_L = 0
RESULT = []
ATT_TREES = []
QI_RANGE = []
IS_CAT = []


class Partition:

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

    def __len__(self):
        """
        return the number of records in partition
        """
        return len(self.member)


def check_L_diversity(partition):
    """check if partition satisfy l-diversity
    return True if satisfy, False if not.
    """
    sa_dict = {}
    if isinstance(partition, Partition):
        records_set = partition.member
    else:
        records_set = partition
    num_record = len(records_set)
    for record in records_set:
        sa_value = list_to_str(record[-1])
        try:
            sa_dict[sa_value] += 1
        except KeyError:
            sa_dict[sa_value] = 1
    if len(sa_dict.keys()) < GL_L:
        return False
    for sa in sa_dict.keys():
        # if any SA value appear more than |T|/l,
        # the partition does not satisfy l-diversity
        if sa_dict[sa] > 1.0 * num_record / GL_L:
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
        normWidth = get_normalized_width(partition, i)
        if normWidth > max_witdh:
            max_witdh = normWidth
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


def find_median(frequency):
    """find the middle of the partition
    return splitVal
    """
    splitVal = ''
    nextVal = ''
    value_list = frequency.keys()
    value_list.sort(cmp=cmp_str)
    total = sum(frequency.values())
    middle = total / 2
    if middle < GL_L:
        print "Error: size of group less than 2*L"
        return '', ''
    index = 0
    split_index = 0
    for i, t in enumerate(value_list):
        index += frequency[t]
        if index >= middle:
            splitVal = t
            split_index = i
            break
    else:
        print "Error: cannot find splitVal"
    try:
        nextVal = value_list[split_index + 1]
    except IndexError:
        nextVal = splitVal
    return (splitVal, nextVal)


def split_numeric_value(numeric_value, splitVal):
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
            rvalue = splitVal + ',' + high
        return lvalue, rvalue


def anonymize(partition):
    """
    Main procedure of mondrian_l_diversity.
    recursively partition groups until not allowable.
    """
    if len(partition) < GL_L * 2:
        RESULT.append(partition)
        return
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
            frequency = frequency_set(partition, dim)
            (splitVal, nextVal) = find_median(frequency)
            if splitVal == '':
                print "Error: splitVal= null"
                pdb.set_trace()
            middle_pos = ATT_TREES[dim].dict[splitVal]
            lmiddle = pmiddle[:]
            rmiddle = pmiddle[:]
            temp = pmiddle[dim].split(',')
            lmiddle[dim], rmiddle[dim] = split_numeric_value(pmiddle[dim], splitVal)
            left_width = pwidth[:]
            right_width = pwidth[:]
            left_width[dim] = (pwidth[dim][0], middle_pos)
            right_width[dim] = (ATT_TREES[dim].dict[nextVal], pwidth[dim][1])
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
            anonymize(Partition(lhs, left_width, lmiddle))
            anonymize(Partition(rhs, right_width, rmiddle))
            return
        else:
            # normal attributes
            if partition.middle[dim] != '*':
                splitVal = ATT_TREES[dim][partition.middle[dim]]
            else:
                splitVal = ATT_TREES[dim]['*']
            if len(splitVal.child) == 0:
                partition.allow[dim] = 0
                continue
            sub_node = [t for t in splitVal.child]
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
                    wtemp[dim] = sub_node[i].support
                    mtemp[dim] = sub_node[i].value
                    anonymize(Partition(sub_partition, wtemp, mtemp))
                return
            else:
                partition.allow[dim] = 0
                continue
    RESULT.append(partition)


def init(att_trees, data, L):
    """
    resset global variables
    """
    global GL_L, RESULT, QI_LEN, ATT_TREES, QI_RANGE, IS_CAT
    ATT_TREES = att_trees
    for gen_tree in att_trees:
        if isinstance(gen_tree, NumRange):
            IS_CAT.append(False)
        else:
            IS_CAT.append(True)
    QI_LEN = len(data[0]) - 1
    GL_L = L
    RESULT = []
    QI_RANGE = []


def mondrian_l_diversity(att_trees, data, L):
    """
    Mondrian for l-diversity.
    This fuction support both numeric values and categoric values.
    For numeric values, each iterator is a mean split.
    For categoric values, each iterator is a split on GH.
    The final result is returned in 2-dimensional list.
    """
    print "L=%d" % L
    init(att_trees, data, L)
    middle = []
    result = []
    wtemp = []
    for i in range(QI_LEN):
        if IS_CAT[i] is False:
            QI_RANGE.append(ATT_TREES[i].range)
            wtemp.append((0, len(ATT_TREES[i].sort_value) - 1))
            middle.append(ATT_TREES[i].value)
        else:
            QI_RANGE.append(ATT_TREES[i]['*'].support)
            wtemp.append(ATT_TREES[i]['*'].support)
            middle.append('*')
    whole_partition = Partition(data, wtemp, middle)
    anonymize(whole_partition)
    ncp = 0.0
    for partition in RESULT:
        rncp = 0.0
        for i in range(QI_LEN):
            rncp += get_normalized_width(partition, i)
        gen_result = partition.middle
        for i in range(len(partition)):
            result.append(gen_result[:])
        rncp *= len(partition)
        ncp += rncp
    ncp /= QI_LEN
    ncp /= len(data)
    ncp *= 100
    if __DEBUG:
        print "size of partitions"
        print len(RESULT)
        # print [len(t) for t in RESULT]
        print "NCP = %.2f %%" % ncp
    return result
