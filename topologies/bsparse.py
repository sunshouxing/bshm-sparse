"""
Word count topology
"""

from streamparse import Grouping, Topology, JavaSpout

from bolts import LogInfoBolt


class WordCount(Topology):
    bshm_spout = JavaSpout.spec(
        name="bshm-spout",
        full_class_name="bshm.spouts.BshmSpout",
        args_list=[],
        outputs=["tdms"]
    )

    log_info_bolt = LogInfoBolt.spec(inputs={bshm_spout: Grouping.LOCAL_OR_SHUFFLE}, par=1)
