# Vert.x Event Bus Bridge Examples

## Overview
Demonstrates the use of the SockJS EventBus bridge and the [Twitter Rx client](https://github.com/shekhargulati/rx-tweet-stream)
to build a real-time Twitter status streaming web application.

## Build
```bash
mvn clean package vertx:package
```

## Run In Development Mode
```bash
mvn compile vertx:run
```

## Run Application
```bash
java -jar target/extended-eventbus-<VERSION>.jar run com.redhat.labs.vertx.eventbus.MainVerticle --conf config.json
```

## Configuration
* Sign Up for [Twitter Developers](https://dev.twitter.com/)
* Create a new [Twitter App](https://apps.twitter.com/)
* Copy `config.example.json` to `config.json`
* Replace the values in the `config.json` file with the settings from your Twitter application profile.