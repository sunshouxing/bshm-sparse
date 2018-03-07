from streamparse import Bolt


class LogInfoBolt(Bolt):

    def process(self, tup):
        self.log(tup.values)
