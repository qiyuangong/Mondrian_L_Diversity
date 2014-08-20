#!/usr/bin/env python
#coding=utf-8

import heapq
import pdb


__DEBUG = True
gl_QI_len = 10
gl_L = 0
gl_result = []
gl_att_trees = []
gl_QI_range = []


class Partition:

    """Class for Group, which is used to keep records 
    Store tree node in instances.
    self.width width of this partition on each domain
    self.middle save the generalization result of this partition
    self.member: records in group
    """

    def __init__(self, data, width, middle):
        """
        split_tuple = (index, low, high)
        """
        self.width = width[:]
        self.middle = middle[:]
        self.member = data[:]


def check_L_diversity(partition):
    """check if partition satisfy l-diversity
    """
    sa_dict = {}
    if isinstance(partition, Partition):
        ltemp = partition.member
    else:
        ltemp = partition
    for temp in partition.member:
        stemp = ';'.join(temp)
        try:
            sa_dict[stemp] += 1
        except:
            sa_dict[stemp] = 1
        if len(sa_dict) >= gl_L:
            return True
    return False


def cmp_str(element1, element2):
    """compare number in str format correctley
    """
    return cmp(int(element1), int(element2))


def getNormalizedWidth(partition, index):
    """return Normalized width of partition
    similar to NCP
    """
    width = partition.width[index]
    return width * 1.0 / gl_QI_range[index]


def choose_dimension(partition):
    """chooss dim with largest normWidth
    """
    # max_wi
    max_witdh = -1
    max_dim = -1
    for i in range(gl_QI_len):
        normWidth = getNormalizedWidth(partition, i)
        if normWidth > max_witdh:
            max_witdh = normWidth
            max_dim = i
    # if __DEBUG and max_witdh == 0:
    #     print "all QI values are equal"
    return max_dim


def frequency_set(partition, dim):
    """get the frequency_set of partition on dim
    """
    value_set = set()
    frequency = {}
    for record in partition.member:
        try:
            if record[dim] in value_set:
                frequency[record[dim]] += 1
            else:
                frequency[record[dim]] = 1
                value_set.add(record[dim])
        except:
            pdb.set_trace()
    return frequency


def find_median(frequency):
    """find the middle of the partition, return left width, righth width and splitVal
    """
    splitVal = ''
    value_list = frequency.keys()
    value_list.sort(cmp=cmp_str)
    total = sum(frequency.values())
    middle = total / 2
    if middle < gl_L:
        print "Error: size of group less than 2*K"
        return ''
    # if __DEBUG:
    #     print 'total = %d' % total
    #     print 'middle = %d' % middle
    #     pdb.set_trace()
    index = 0
    for t in value_list:
        index += frequency[t]
        if index >= middle:
            splitVal = t
            break
    else:
        print "Error: cannot find splitVal"
    return splitVal


def anonymize(partition):
    """recursively partition groups until not allowable
    """
    if len(partition.member) < 2*gl_L:
        gl_result.append(partition)
        return
    dim = choose_dimension(partition)
    pwidth = partition.width[:]
    pmiddle = partition.middle[:]
    if dim == -1:
        print "Error: dim=-1"
        pdb.set_trace()
    if isinstance(gl_att_trees[dim], NumRange):
        # numeric attributes
        frequency = frequency_set(partition, dim)
        splitVal = find_median(frequency)
        if splitVal == '':
            print "Error: splitVal= null"
            pdb.set_trace()
        middle_pos = gl_att_trees[dim].dict[splitVal]
        lmiddle = pmiddle[:]
        lwidth = pwidth[:]
        lwidth[dim] = middle_pos + 1
        lmiddle[dim] 
        temp = pmiddle[dim].split(',')
        temp[-1] = splitVal
        lmiddle[dim] = ','.join(temp)
        rwidth = pwidth[:]
        rwidth[dim] = width[dim] - middle_pos - 1
        temp = pmiddle[dim].split(',')
        temp[0] = splitVal
        lmiddle[dim] = ','.join(temp)
        lhs = []
        rhs = []
        for temp in partition.member:
            pos = gl_att_trees[dim].dict[temp[dim]]
            if pos <= middle_pos:
                # lhs = [low, means]
                lhs.append(temp)
            else:
                # rhs = (means, high)
                rhs.append(temp)
        if len(lhs) < gl_L or len(rhs) < gl_L:
            gl_result.append(partition)
            return
        # anonymize sub-partition
        anonymize(Partition(lhs,lwidth,lmiddle))
        anonymize(Partition(rhs,rwidth,rmiddle))
    else:
        # normal attributes
        sub_partition = [] * gl_QI_len
        sub_node = [t for t in partition.middle.child]
        for temp in partition.member:
            qid_value =  temp[dim]
            for i, node in enumerate(sub_node):
                try:
                    node.cover[qid_value]
                    sub_partition[i].append(temp)
                except:
                    continue
        for p in sub_partition:
            anonymize(Partition(p))


def mondrian_l_diversity(att_trees, data, L):
    """
    """
    global gl_L, gl_result, gl_QI_len, gl_att_trees, gl_QI_range
    gl_att_trees = att_trees
    middle = []
    gl_QI_len = len(data[0])-1
    gl_L = L
    gl_result = []
    result = []
    gl_QI_range = []
    for i in range(gl_QI_len):
        if isinstance(gl_att_trees[i], NumRange):
            gl_QI_range.append(gl_att_trees[i].range)
        else:
            gl_QI_range.append(gl_att_trees.support)
        middle.append(gl_att_trees[i].value)
    partition = Partition(data, gl_QI_range[:], middle)
    anonymize(partition)
    for p in gl_result:
        for temp in p.member:
            temp = partition.middle[:]
            result.append(temp)
    if __DEBUG:
        print "size of partitions"
        print [len(t.member) for t in gl_result]
        pdb.set_trace()
    return result