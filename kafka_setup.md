## KAFKA DEPLOYMENT INSTRUCTIONS

### INSTALLATION INSTRUCTIONS
#### Run these commands:
Java installation
```shell
sudo apt update
sudo apt -y install openjdk-8-jdk-headless
```

Install Zookeeper
```shell
sudo apt-get -y install zookeeperd
```

Use the most recent stable version of Kafka for download
```shell
wget "http://apache.mirrors.pair.com/kafka/2.2.0/kafka_2.12-2.2.0.tgz"
```

Create directories and unpack Kafka
```shell
sudo mkdir /opt/kafka
sudo tar -xvzf kafka_2.12-2.2.0.tgz --directory /opt/kafka --strip-components 1
rm kafka_2.12-2.2.0.tgz
```

Create directory for Kafka logs
```shell
sudo mkdir /var/lib/kafka
sudo mkdir /var/lib/kafka/data
```
OPTIONAL: install python3 and kafka driver to create python kafka clients
```shell
sudo apt -y install python3-pip
pip3 install kafka-python
```

### ZOOKEEPER CONFIG

Stop the Zookeeper service before making config changes. Use the following command:
```shell
sudo systemctl stop zookeeper.service
```

If you are setting up a Zookeeper cluster, __stop all the Zookeepers__ before making config changes.
Start Zookeeper services on the servers after you have made config changes on all servers.

Open ```/etc/zookeeper/conf/zoo.cfg``` and add the ip addresses of all zookeeper servers you want in the cluster including the current server.

It should look like this:
```
server.1=10.142.0.59:2888:3888
server.2=10.142.0.60:2888:3888
server.3=10.142.0.61:2888:3888
```
so on...

Start Zookeeper services on the servers after you have made config changes on all servers:
```shell
sudo systemctl start zookeeper.service
```

### KAFKA CONFIG

If you have not configured the zookeeper yet, follow the instructions to __set up zookeeper first__.

Edit ```/opt/kafka/config/server.properties``` to make the following config changes:

Under ```Server Basics``` set __unique ```broker.id``` value for every server__ (not zero). Example:
```
broker.id=60
```

Add the following two commands below the config ```broker.id``` in ```server.properties```:
```
#Enables deletion of topics
delete.topic.enable = true
```

Under ```Socket Server Settings``` uncomment ```#listeners=PLAINTEXT://:9092``` and add the current server host ip address. 
Example: 
```
listeners=PLAINTEXT://10.142.0.60:9092
```

Under ```Log Basics``` set: 
```
log.dirs=/var/lib/kafka/data/
```

Under ```Log Retention Policy``` set: 
```
log.retention.bytes=104857600
```

Under ```Zookeeper``` add comma separated zookeeper ips with ports to ```zookeeper.connect```.
Example: 
```
zookeeper.connect=10.142.0.59:2181,10.142.0.60:2181,10.142.0.61:2181
``` 
Save this file and exit.

To start Kafka use the following command on all servers:
```shell
sudo /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties &
```
