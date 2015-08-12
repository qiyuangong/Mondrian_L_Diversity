"""
run mondrian_l_diversity with given parameters
"""

# !/usr/bin/env python
# coding=utf-8
from mondrian_l_diversity import mondrian_l_diversity
from utils.read_data import read_data, read_tree
import sys
import pdb
# Poulis set k=25, m=2 as default!

if __name__ == '__main__':
    GL_L = 10
    try:
        GL_L = int(sys.argv[1])
    except:
        pass
    ATT_TREES = read_tree()
    # read record
    DATA = read_data()
    # remove duplicate items
    print "Begin Partition"
    # anonymized dataset is stored in RESULT
    RESULT = mondrian_l_diversity(ATT_TREES, DATA, GL_L)
    print "Finish Partition!!"
