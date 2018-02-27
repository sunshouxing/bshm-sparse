# -*- coding: utf-8 -*-

from streamparse import Bolt, Tuple


class TDMSParseBolt(Bolt):
    """
    Storm bolt to parse LabVIEW's TDMS messages.
    """
    def process(self, tup):
        self.log(type(tup.values))
