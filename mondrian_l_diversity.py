#!/usr/bin/env python
#coding=utf-8


import pdb
from models.numrange import NumRange
from models.gentree import GenTree


__DEBUG = True
gl_QI_len = 10
gl_L = 0
gl_result = []
gl_att_trees = []
gl_QI_range = []


class Partition:

    """Class for Group, which is used to keep records 
    Store tree node in instances.
    self.width: width of this partition on each domain
    self.middle: save the generalization result of this partition
    self.member: records in group
    self.allow: 0 donate that not allow to split, 1 donate can be split
    """

    def __init__(self, data, width, middle):
        """
        initialize with data, width and middle
        """
        self.member = data[:]
        self.width = width[:]
        self.middle = middle[:]
        self.allow = [1] * gl_QI_len


def check_L_diversity(partition):
    """check if partition satisfy l-diversity
    """
    sa_dict = {}
    if isinstance(partition, Partition):
        ltemp = partition.member
    else:
        ltemp = partition
    for temp in ltemp:
        stemp = ';'.join(temp[-1])
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
    """chooss dim with largest normlized Width
    """
    max_witdh = -1
    max_dim = -1
    for i in range(gl_QI_len):
        if partition.allow[i] == 0:
            continue
        normWidth = getNormalizedWidth(partition, i)
        if normWidth > max_witdh:
            max_witdh = normWidth
            max_dim = i
    if max_witdh > 1:
        pdb.set_trace()
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
    """find the middle of the partition, 
    return splitVal
    """
    splitVal = ''
    value_list = frequency.keys()
    value_list.sort(cmp=cmp_str)
    total = sum(frequency.values())
    middle = total / 2
    if middle < gl_L:
        print "Error: size of group less than 2*K"
        return ''
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
    return (splitVal, split_index)


def anonymize(partition):
    """recursively partition groups until not allowable
    """
    global gl_result
    if sum(partition.allow) == 0 or len(partition.member) < 2*gl_L:
        gl_result.append(partition)
        return
    pwidth = partition.width[:]
    pmiddle = partition.middle[:]
    for i in range(gl_QI_len):
        dim = choose_dimension(partition)
        if dim == -1:
            print "Error: dim=-1"
            pdb.set_trace()
        if isinstance(gl_att_trees[dim], NumRange):
            # numeric attributes
            frequency = frequency_set(partition, dim)
            (splitVal, split_index) = find_median(frequency)
            if splitVal == '':
                print "Error: splitVal= null"
                pdb.set_trace()
            middle_pos = gl_att_trees[dim].dict[splitVal]
            lmiddle = pmiddle[:]
            lmiddle[dim] 
            temp = pmiddle[dim].split(',')
            temp[-1] = splitVal
            lmiddle[dim] = ','.join(temp)
            rmiddle = pmiddle[:]
            temp = pmiddle[dim].split(',')
            temp[0] = splitVal
            rmiddle[dim] = ','.join(temp)
            lhs = []
            rhs = []
            lcount = rcount  = 0
            for temp in partition.member:
                pos = gl_att_trees[dim].dict[temp[dim]]
                if pos <= middle_pos:
                    # lhs = [low, means]
                    lhs.append(temp)
                    lcount += 1
                else:
                    # rhs = (means, high)
                    rhs.append(temp)
                    rcount += 1
            lwidth = pwidth[:]
            rwidth = pwidth[:]
            lwidth[dim] = split_index
            rwidth[dim] = pwidth[dim] - split_index
            if check_L_diversity(lhs) == False or  check_L_diversity(rhs) == False:
                partition.allow[dim] = 0
                continue
            # anonymize sub-partition
            anonymize(Partition(lhs,lwidth,lmiddle))
            anonymize(Partition(rhs,rwidth,rmiddle))
            return
        else:
            # normal attributes
            if partition.middle[dim] != '*':
                try:
                    splitVal = gl_att_trees[dim][partition.middle[dim]]
                except:
                    pdb.set_trace()
            else:
                splitVal = gl_att_trees[dim]['*']
            sub_node = [t for t in splitVal.child]    
            sub_partition = []    
            for i in range(len(sub_node)):
                sub_partition.append([])
            for temp in partition.member:
                qid_value =  temp[dim]
                for i, node in enumerate(sub_node):
                    try:
                        node.cover[qid_value]
                        sub_partition[i].append(temp)
                        break
                    except:
                        continue
            flag = True
            for p in sub_partition:
                if check_L_diversity(p) == False:
                    flag = False
                    break
            if flag:
                for i,p in enumerate(sub_partition):
                    wtemp = pwidth[:]
                    mtemp = pmiddle[:]
                    wtemp[dim] = sub_node[i].support
                    mtemp[dim] = sub_node[i].value
                    anonymize(Partition(p, wtemp, mtemp))
                return
            else:
                partition.allow[dim] = 0
                continue
    gl_result.append(partition)


def mondrian_l_diversity(att_trees, data, L):
    """
    """
    print "L=%d" % L
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
            middle.append(gl_att_trees[i].value)
        else:
            gl_QI_range.append(gl_att_trees[i]['*'].support)
            middle.append(gl_att_trees[i]['*'].value)
    partition = Partition(data, gl_QI_range[:], middle)
    anonymize(partition)
    ncp = 0.0
    for p in gl_result:
        rncp = 0.0
        for i in range(gl_QI_len):
            rncp += getNormalizedWidth(p, i)
        temp = p.middle
        for i in range(len(p.member)):
            result.append(temp[:])
        rncp *= len(p.member)
        ncp += rncp
    ncp /= gl_QI_len
    ncp /= len(data)
    ncp *= 100
    if __DEBUG:
        print "size of partitions"
        print [len(t.member) for t in gl_result]
        print "NCP = %.2f %%" % ncp
        pdb.set_trace()
    return result