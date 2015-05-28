#!/usr/bin/env python
# coding=utf-8

# Read data and read tree fuctions for INFORMS data
# user att ['DUID', 'PID', 'DUPERSID', 'DOBMM', 'DOBYY', 'SEX', 'RACEX', 'RACEAX', 'RACEBX', 'RACEWX', 'RACETHNX', 'HISPANX', 'HISPCAT', 'EDUCYEAR', 'Year', 'marry', 'income', 'poverty']
# condition att ['DUID', 'DUPERSID', 'ICD9CODX', 'year']
from models.gentree import GenTree
from models.numrange import NumRange
import pickle


__DEBUG = False
gl_useratt = ['DUID', 'PID', 'DUPERSID', 'DOBMM', 'DOBYY', 'SEX',
              'RACEX', 'RACEAX', 'RACEBX', 'RACEWX', 'RACETHNX',
              'HISPANX', 'HISPCAT', 'EDUCYEAR', 'Year', 'marry', 'income', 'poverty']
gl_conditionatt = ['DUID', 'DUPERSID', 'ICD9CODX', 'year']
# Only 5 relational attributes and 1 transaction attribute are selected (according to Poulis's paper)
# DOBMM DOBYY RACEX, EDUCYEAR, income
gl_attlist = [3, 4, 6, 13, 16]
gl_if_cat = [True, True, True, True, False]


def cmp_str(element1, element2):
    """compare number in str format correctley
    """
    return cmp(int(element1), int(element2))


def read_tree():
    """read tree from data/tree_*.txt, store them in att_tree
    """
    att_names = []
    att_trees = []
    for t in gl_attlist:
        att_names.append(gl_useratt[t])
    for i in range(len(att_names)):
        if gl_if_cat[i]:
            att_trees.append(read_tree_file(att_names[i]))
        else:
            att_trees.append(pickle_static(gl_attlist[i]))
    return att_trees


def pickle_static(index):
    """pickle sorted values of BMS-WebView-2 to BMS_Static_value.pickle
    """
    userfile = open('data/demographics.csv', 'rU')
    need_static = False
    support = {}
    try:
        static_file = open('data/' + gl_useratt[index] + '_Static_value.pickle', 'rb')
        print "Data exist..."
        (support, sort_value) = pickle.load(static_file)
    except:
        need_static = True
        static_file = open('data/' + gl_useratt[index] + '_Static_value.pickle', 'wb')
        print "Pickle Data..."
        for i, line in enumerate(userfile):
            line = line.strip()
            if i == 0:
                continue
            # ignore first line of csv
            row = line.split(',')
            try:
                support[row[index]] += 1
            except:
                support[row[index]] = 1
        sort_value = support.keys()
        sort_value.sort(cmp=cmp_str)
        pickle.dump((support, sort_value), static_file)
    static_file.close()
    userfile.close()
    result = NumRange(sort_value, support)
    return result


def read_tree_file(treename):
    """read tree data from treename
    """
    leaf_to_path = {}
    att_tree = {}
    prefix = 'data/informs_'
    postfix = ".txt"
    treefile = open(prefix + treename + postfix, 'rU')
    att_tree['*'] = GenTree('*')
    if __DEBUG:
        print "Reading Tree" + treename
    for line in treefile:
        # delete \n
        if len(line) <= 1:
            break
        line = line.strip()
        temp = line.split(';')
        # copy temp
        temp.reverse()
        for i, t in enumerate(temp):
            isleaf = False
            if i == len(temp) - 1:
                isleaf = True
            # try and except is more efficient than 'in'
            try:
                att_tree[t]
            except:
                att_tree[t] = GenTree(t, att_tree[temp[i - 1]], isleaf)
    if __DEBUG:
        print "Nodes No. = %d" % att_tree['*'].support
    treefile.close()
    return att_tree


def read_data(flag=0):
    """read microda for *.txt and return read data
    """
    """read microda for *.txt and return read data"""
    data = []
    userfile = open('data/demographics.csv', 'rU')
    conditionfile = open('data/conditions.csv', 'rU')
    userdata = {}
    # We selet 3,4,5,6,13,15,15 att from demographics05, and 2 from condition05
    print "Reading Data..."
    for i, line in enumerate(userfile):
        line = line.strip()
        # ignore first line of csv
        if i == 0:
            continue
        row = line.split(',')
        row[2] = row[2][1:-1]
        try:
            userdata[row[2]].append(row)
        except:
            userdata[row[2]] = row
    conditiondata = {}
    for i, line in enumerate(conditionfile):
        line = line.strip()
        # ignore first line of csv
        if i == 0:
            continue
        row = line.split(',')
        row[1] = row[1][1:-1]
        row[2] = row[2][1:-1]
        try:
            conditiondata[row[1]].append(row)
        except:
            conditiondata[row[1]] = [row]
    hashdata = {}
    for k, v in userdata.iteritems():
        if k in conditiondata:
            # ingnore duplicate values
            temp = set()
            for t in conditiondata[k]:
                temp.add(t[2])
            hashdata[k] = []
            for i in range(len(gl_attlist)):
                index = gl_attlist[i]
                hashdata[k].append(v[index])
            stemp = list(temp)
            # sort values
            stemp.sort()
            hashdata[k].append(stemp[:])
    for k, v in hashdata.iteritems():
        data.append(v)
    userfile.close()
    conditionfile.close()
    return data
