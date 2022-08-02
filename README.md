![Maven Build](https://github.com/samagra-comms/broadcast-transformer/actions/workflows/build.yml/badge.svg)
![Docker Build](https://github.com/samagra-comms/broadcast-transformer/actions/workflows/docker-build-push.yml/badge.svg)

# Overview
Broadcast transformer convert a single message to multiple messages for each user & accordingly and sends the messages to outbound for broadcast.

# Getting Started

## Prerequisites

* java 11 or above
* docker
* kafka
* postgresql
* redis
* fusion auth
* lombok plugin for IDE
* maven

## Build
* build with tests run using command **mvn clean install -U**
* or build without tests run using command **mvn clean install -DskipTests**

# Detailed Documentation
[Click here](https://uci.sunbird.org/use/developer/uci-basics)