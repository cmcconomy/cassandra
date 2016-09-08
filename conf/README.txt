Required configuration files
============================

cassandra.yaml: main Cassandra configuration file
logback.xml: logback configuration file for Cassandra server


Optional configuration files
============================

cassandra-overlay.yaml: overlay Cassandra configuration file 
                      (overrides settings in cassandra.yaml) 
cassandra-env.sh: Environment variables
cassandra-topology.properties: used by PropertyFileSnitch
cassandra-rackdc.properties: used by GossipingPropertyFileSnitch
