"""
Word count topology
"""

from streamparse import Grouping, Topology, JavaSpout

from bolts import LogInfoBolt, TDMSParseBolt


class WordCount(Topology):
    bshm_spout = JavaSpout.spec(
        name="bshm-spout",
        full_class_name="bshm.spouts.BshmSpout",
        args_list=[],
        outputs=["tdms"]
    )

    tdms_parse_bolt = TDMSParseBolt.spec(par=1, inputs={
        bshm_spout: Grouping.LOCAL_OR_SHUFFLE
    })

    log_info_bolt = LogInfoBolt.spec(par=1, inputs=[
        tdms_parse_bolt['FCXF-X-04-S01'],
        tdms_parse_bolt['FCXF-X-04-S02'],
        tdms_parse_bolt['FCXF-X-04-S03'],
        tdms_parse_bolt['FCXF-X-04-S04']
    ])
