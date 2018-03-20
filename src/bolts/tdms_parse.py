# -*- coding: utf-8 -*-

import base64
import io
import time

from nptdms import TdmsFile
from streamparse import Bolt, Stream


class TDMSParseBolt(Bolt):
    # group channels as output streams' name
    channels = (
        'FCXF-X-02-T01',
        'FCXF-X-02-T02',
        'FCXF-X-02-T03',
        'FCXF-X-02-T04',
        'FCXF-X-03-T01',
        'FCXF-X-03-T02',
        'FCXF-X-03-T03',
        'FCXF-X-03-T04',
        'FCXF-X-03-T05',
        'FCXF-X-03-T06',
        'FCXF-X-04-T01',
        'FCXF-X-04-T02',
        'FCXF-X-04-T03',
        'FCXF-X-04-T04',
        'FCXF-X-02-S01',
        'FCXF-X-02-S02',
        'FCXF-X-02-S03',
        'FCXF-X-02-S04',
        'FCXF-X-02-A01',
        'FCXF-X-03-A01',
        'FCXF-X-03-A02',
        'FCXF-X-04-A01',
        'FCXF-X-03-S05',
        'FCXF-X-03-S06',
        'FCXF-X-03-S01',
        'FCXF-X-03-S02',
        'FCXF-X-03-S03',
        'FCXF-X-03-S04',
        'FCXF-X-04-S01',
        'FCXF-X-04-S02',
        'FCXF-X-04-S03',
        'FCXF-X-04-S04'
    )

    # channel properties as tuple fields
    # tuple_fields = ('timestamp', 'time_offset', 'time_increment', 'samples', 'channel_name', 'module_name', 'data')

    # output streams declare
    # outputs = [Stream(tuple_fields, channel) for channel in channels]

    def process(self, tup):
        data = tup.values[0]

        try:
            # decode the base64 encoded data
            decoded_data = base64.b64decode(data)

            # the tdms data starts with 'TDSm'
            i = decoded_data.index('TDSm')

            data_stream = io.BytesIO(decoded_data[i:])
            tdms_file = TdmsFile(data_stream)
        except Exception as error:
            self.log(error, 'error')
        else:
            for tup in self._parse(tdms_file):
                self.emit(tup, stream=tup[-3])
                self.emit(tup, stream='ALL-CHANNELS')

    def _parse(self, tdms_file):
        for group in tdms_file.groups():
            for channel in tdms_file.group_channels(group):
                if channel.channel in self.channels:
                    # acquire this channel's 'wf_start_time' property
                    # and get its timestamp value for JSON serialize
                    start_time = channel.property('wf_start_time')
                    timestamp = time.mktime(start_time.timetuple())
                    tup = [timestamp]

                    # acquire this channel's other properties
                    others = [v for k, v in channel.properties.items() if k != 'wf_start_time']
                    tup.extend(others)

                    # acquire channel data
                    data = channel.data.tolist()
                    tup.append(data)

                    yield tup

# EOF
