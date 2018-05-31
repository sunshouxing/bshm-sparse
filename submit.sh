storm jar _build/bsparse-0.0.1-SNAPSHOT-standalone.jar \
    org.apache.storm.flux.Flux \
    --remote --filter filters/baofu.properties \
    topology.yaml
