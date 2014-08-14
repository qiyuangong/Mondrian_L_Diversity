#!/usr/bin/env python
#coding=utf-8
from mondrian_l_diversity import mondrian_l_diversity
from utils.read_data import read_data, read_tree
from utils.save_result import save_to_file
import sys
import pdb
# Poulis set k=25, m=2 as default!

if __name__ == '__main__':
    K = 10
    try:
        K = int(sys.argv[1])
    except:
        pass
    att_trees = read_tree()
    #read record
    data = read_data()
    # remove duplicate items
    print "Begin Partition"
    result = mondrian_l_diversity(att_trees, data, K)
    save_to_file(result)
    print "Finish Partition!!"
