# -*- coding: utf-8 -*-

import base64
import io
import time

from nptdms import TdmsFile
import numpy as np
from streamparse import Bolt, Stream


class TDMSParseBolt(Bolt):

    def process(self, tup):
        data = tup.values[0]

        try:
            # decode the base64 encoded data
            decoded_data = base64.b64decode(data)

            # the tdms data starts with 'TDSm'
            i = decoded_data.index('TDSm')

            data_stream = io.BytesIO(decoded_data[i:])
            tdms_file = TdmsFile(data_stream)

            for tup in self._parse(tdms_file):
                channel = tup[-2]
                if channel[-3] == 'S':
                    self.emit(tup, stream=channel)
                else:
                    self.emit(tup, stream='no-strains')
        except Exception as error:
            self.log(error, 'error')


    def _parse(self, tdms_file):
        for group in tdms_file.groups():
            for channel in tdms_file.group_channels(group):
                if channel.channel.startswith(('FCXF', 'NLHQ')):
                    # acquire this channel's 'wf_start_time' property
                    # and get its timestamp value for JSON serialize
                    start_time = channel.property('wf_start_time')
                    timestamp = time.mktime(start_time.timetuple())
                    tup = [timestamp]

                    # acquire this channel's other properties
                    tup.append(channel.property('wf_start_offset'))
                    tup.append(channel.property('wf_increment'))
                    tup.append(channel.property('NI_ChannelName'))
                    tup.append(np.nan_to_num(channel.data).tolist())

                    yield tup
