(defproject bsparse "0.0.1-SNAPSHOT"
  :java-source-paths ["src/java"]
  :aot :all
  :resource-paths ["_resources"]
  :target-path "_build"
  :min-lein-version "2.0.0"
  :jvm-opts ["-client"]
  :dependencies [
                 [org.apache.storm/storm-core "1.1.1"]
                 [org.apache.storm/flux-core "1.1.1"]
                 [org.apache.storm/storm-kafka "1.1.1"]
                 [org.apache.storm/storm-mongodb "1.1.1"]
                 [org.apache.kafka/kafka-clients "0.10.0.0"]
                 [
                    org.apache.kafka/kafka_2.10 "0.10.0.0"
                    :exclusions [org.slf4j/slf4j-log4j12 log4j/log4j org.apache.zookeeper/zookeeper]
                 ]
                ]
  :jar-exclusions     [#"log4j\.properties" #"org\.apache\.storm\.(?!flux|kafka)" #"META-INF" #"meta-inf" #"\.yaml"]
  :uberjar-exclusions [#"log4j\.properties" #"org\.apache\.storm\.(?!flux|kafka)" #"META-INF" #"meta-inf" #"\.yaml"]
  )
