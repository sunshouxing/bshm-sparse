# -*- coding: utf-8 -*-
__author__ = 'SUN Shouwang'

import numpy as np
import pandas as pd
from streamparse.bolt import Bolt
from streamparse import Stream


class DetrendBolt(Bolt):
    tup_fields = ('timestamp', 'time_offset', 'time_increment', 'samples', 'channel_name', 'module_name', 'data')
    outputs = [Stream(fields=tup_fields, name='detrend')]

    def initialize(self, storm_conf, context):
        """
        receive parameters set in topology definition from storm_conf argment
        :param dict storm_conf: the Storm configuration for this component
        :param dict context: information about the component’s place within the topology
        """
        self.min_tup_num = storm_conf.get('min_tup_num', 3)

    def process(self, tup):
        """
        step 1: receive tup and convert to pandas.Dataframe;
        step 2: call self._detrend() function to remove the mode value from the original data;
        stip 3: convert resulted pandas.Dataframe to list and send out.
        :param streamparse.Tuple, tup: tup.values =
                [timestamp, time_offset, time_increment, channel_name, data]
        :return streamparse.Tuple, tup: tup.values =
                [timestamp, time_offset, time_increment, channel_name, data]
        """
        timestamp = tup.values[0]
        time_increment = tup.values[2]
        channel_name = tup.values[3]
        data = tup.values[4]

        start_time = pd.Timestamp(timestamp, unit='s', tz='UTC')
        periods = data.__len__()
        freq = '{}ms'.format(int(time_increment / 0.001))

        index = pd.MultiIndex.from_product(
            [[start_time], pd.date_range(start=start_time, periods=periods, freq=freq)],
            names=['start_time', 'timestamp']
        )
        df = pd.DataFrame(data=data, index=index, columns=[channel_name])

        try:
            self.history = self.history.combine_first(df)
        except AttributeError:
            self.history = df
            self.res = list(tup.values)

        for df in self._detrend():
            self.res[0] = df.index[0].timestamp()
            self.res[4] = np.nan_to_num(df[channel_name].values).tolist()
            self.emit(self.res, stream='detrend')

    def _detrend(self):
        """
        remove the mode value from the original data
        :return pandas.Dataframe df: de-trended signal segments
        """
        start_time_index = self.history.index.get_level_values(level='start_time')
        unique_values = start_time_index.unique()
        while unique_values.__len__() >= self.min_tup_num:
            mode = self.history.mode()
            df = self.history.loc[unique_values[0]] - mode.loc[0].values
            self.history.drop(index=unique_values[0], level='start_time', inplace=True)
            unique_values = unique_values.delete(0)
            yield df
# EOF
