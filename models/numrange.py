#!/usr/bin/env python
#coding=utf-8

# Range for numeric type
class NumRange(object):

    """Class for Generalization hierarchies (Taxonomy Tree). 
    Store tree node in instances.
    self.value: node value
    self.level: tree level (top is 0)
    self.support: support
    self.parent: ancestor node list
    self.child: direct successor node list
    self.cover: leaves nodes of current node 
    """

    def __init__(self, sort_value, support):
        self.sort_value = sort_value[:]
        self.support = support.copy()
        self.range = len(sort_value)
