### Kafka Setup
First, install and start Docker Desktop or Docker Engine if you don't already have it. Verify that Docker is set up properly by ensuring that no errors are output when you run docker info in your terminal.

#### Install Confluent CLI
```bash
pip install confluent-kafka
brew install confluentinc/tap/cli
```

#### Start Kafka broker
```bash
confluent local kafka start
```
Paste the printed Plaintext Ports in PLAINTEXT PORTS of config.ini file.

### Create Topics
```bash
confluent local kafka topic create raw-accuracy
confluent local kafka topic create agg-accuracy
```

### Start Kafka Stream Processor
Modify the config.properties file in the IntelliJ project and create the jar file 
```bash
java -jar ckn-streaming-1.0-SNAPSHOT-jar-with-dependencies.jar
```

### Build Producer and Produce Events
```bash
chmod u+x producer.py
./producer.py config.ini
```

### Build Consumer and Consume Events
```bash
chmod u+x consumer.py
./consumer.py config.ini
```

### Terminate Kafka
```bash
confluent local kafka stop
```