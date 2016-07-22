[![Build Status](https://travis-ci.com/intelsdi-x/snap-plugin-publisher-kafka.svg?token=HoxHq3yqBGpySzRd5XUm)](https://travis-ci.com/intelsdi-x/snap-plugin-publisher-kafka)

# snap publisher plugin - Kafka

Allows publishing of data to [Apache Kafka](http://kafka.apache.org)

It's used in the [snap framework](http://github.com/intelsdi-x/snap).

1. [Getting Started](#getting-started)
  * [System Requirements](#system-requirements)
  * [Installation](#installation)
  * [Configuration and Usage](#configuration-and-usage)
2. [Documentation](#documentation)
  * [Quick Start on Kafka](#quick-start-on-kafka)
  * [Examples](#examples)
  * [Roadmap](#roadmap)
3. [Community Support](#community-support)
4. [Contributing](#contributing)
5. [License](#license)
6. [Acknowledgements](#acknowledgements)

## Getting Started

### System Requirements

- Uses [sarama](http://shopify.github.io/sarama/) golang client for Kafka by Shopify

### Installation

#### Download Kafka plugin binary:
You can get the pre-built binaries for your OS and architecture at snap's [GitHub Releases](https://github.com/intelsdi-x/snap/releases) page.

#### To build the plugin binary:
Fork https://github.com/intelsdi-x/snap-plugin-publisher-kafka  
Clone repo into `$GOPATH/src/github.com/intelsdi-x/`:

```
$ git clone https://github.com/<yourGithubID>/snap-plugin-publisher-kafka.git
```

Build the plugin by running make within the cloned repo:
```
$ make
```
This builds the plugin in `/build/rootfs/`

### Configuration and Usage
* Set up the [snap framework](https://github.com/intelsdi-x/snap/blob/master/README.md#getting-started)
* Ensure `$SNAP_PATH` is exported  
`export SNAP_PATH=$GOPATH/src/github.com/intelsdi-x/snap/build`

#### Task Manifest Config

In task manifest, in config section of Kafka publisher the following settings can be declared:

| Key     | Type       | Default value     | Description                |
|---------|------------|-------------------|----------------------------|
| topic   | string     | "snap"            | The topic to send messages |         
| brokers | string     | "localhost:9092"  | Semicolon delimited list of "server:port" brokers </br> Kafka's standard broker communication port is 9092 |

## Documentation

### Quick Start on Kafka

This is a minimal-configuration needed to run the Kafka broker service on Docker

#### Run ZooKeeper server on docker  
Kafka uses ZooKeeper so you need to first start a ZooKeeper server if you don't already have one.
```	
 $ docker run -d --name zookeeper jplock/zookeeper:3.4.6
```
Check if ZooKeeper docker is running:
```	
$ docker ps  

	CONTAINER ID        IMAGE                      COMMAND                CREATED             STATUS              PORTS              NAMES
	9b0ddbdd75cd        jplock/zookeeper:3.4.6     "/opt/zookeeper/bin/   38 seconds ago      Up 38 seconds       2181/tcp, 2888/tcp, 3888/tcp                                        zookeeper
```  

#### Run Kafka server on docker
```
 docker run -d --name kafka --link zookeeper:zookeeper ches/kafka
```

#### Verify the running docker containers:
```	
$ docker ps

	CONTAINER ID        IMAGE                        COMMAND                CREATED             STATUS              PORTS                                                               NAMES
	dfb3cdfb3f87        ches/kafka:latest            "/start.sh"            7 seconds ago       Up 6 seconds        7203/tcp, 9092/tcp                                                  kafka
	9b0ddbdd75cd        jplock/zookeeper:3.4.6       "/opt/zookeeper/bin/   3 minutes ago       Up 3 minutes        2181/tcp, 2888/tcp, 3888/tcp                                        zookeeper
```

#### Get Kafka advertised hostname:
```	
$ docker inspect --format '{{ .NetworkSettings.IPAddress }}' kafka

  172.17.0.14
```

Read more about Kafka on [http://kafka.apache.org](http://kafka.apache.org/documentation.html)

### Examples

Example running mock collector plugin, passthru processor plugin, and writing data to Kafka.

Make sure that your `$SNAP_PATH` is set, if not:
```
$ export SNAP_PATH=<snapDirectoryPath>/build
```
In one terminal window, open the snap daemon (in this case with logging set to 1 and trust disabled):
```
$ $SNAP_PATH/bin/snapd -l 1 -t 0
```
In another terminal window:
Load snap-plugin-collector-mock1 plugin:
```
$ $SNAP_PATH/bin/snapctl plugin load $SNAP_PATH/plugin/snap-plugin-collector-mock1
```
See available metrics for your system:
```
$ $SNAP_PATH/bin/snapctl metric list
```
Load snap-plugin-processor-passthru plugin:
```
$ $SNAP_PATH/bin/snapctl plugin load $SNAP_PATH/plugin/snap-plugin-processor-passthru
```
Load snap-plugin-publisher-kafka plugin:
```
$ $SNAP_PATH/bin/snapctl plugin load build/rootfs/snap-plugin-publisher-kafka
```
Create a task manifest to use Kafka publisher plugin (see [exemplary task](examples/tasks/)):
```
{
    "version": 1,
    "schedule": {
        "type": "simple",
        "interval": "1s"
    },
    "workflow": {
        "collect": {
            "metrics": {
                "/intel/mock/foo": {},
                "/intel/mock/bar": {},
                "/intel/mock/*/baz": {}
            },
            "config": {
                "/intel/mock": {
                    "user": "root",
                    "password": "secret"
                }
            },
            "process": [
                {
                    "plugin_name": "passthru",
                    "publish": [
                        {
                            "plugin_name": "kafka",
                            "config": {
                                "topic": "test",
                                "brokers": "172.17.0.14:9092"
                            }
                        }
                    ]
                }
            ]
        }
    }
}
```


### Roadmap

There isn't a current roadmap for this plugin, but it is in active development. As we launch this plugin, we do not have any outstanding requirements for the next release.

If you have a feature request, please add it as an [issue](https://github.com/intelsdi-x/snap-plugin-publisher-kafka/issues/new) and/or submit a [pull request](https://github.com/intelsdi-x/snap-plugin-publisher-kafka/pulls).

## Community Support
This repository is one of **many** plugins in **snap**, a powerful telemetry framework. See the full project at http://github.com/intelsdi-x/snap To reach out to other users, head to the [main framework](https://github.com/intelsdi-x/snap#community-support)

## Contributing
We love contributions! 

There's more than one way to give back, from examples to blogs to code updates. See our recommended process in [CONTRIBUTING.md](CONTRIBUTING.md).

## License
[snap](http://github.com/intelsdi-x/snap), along with this plugin, is an Open Source software released under the Apache 2.0 [License](LICENSE).

## Acknowledgements

* Author: [Nicholas Weaver](https://github.com/lynxbat)

And **thank you!** Your contribution, through code and participation, is incredibly important to us.
