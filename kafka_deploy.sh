# Installs Java 1.8, Zookeeper (latest) and Kafka 2.2.0 with Scala 2.12.
# Change the tar file name to change kafka (and scala) version.

sudo apt update
sudo apt -y install openjdk-8-jdk-headless
sudo apt-get -y install zookeeperd
wget "http://apache.mirrors.pair.com/kafka/2.2.0/kafka_2.12-2.2.0.tgz"
sudo mkdir /opt/kafka
sudo tar -xvzf kafka_2.12-2.2.0.tgz --directory /opt/kafka --strip-components 1
rm kafka_2.12-2.2.0.tgz
sudo mkdir /var/lib/kafka
sudo mkdir /var/lib/kafka/data
