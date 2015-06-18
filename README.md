#What is Gobroke?#
Gobroke is an implementation of [MQTT 3.1](http://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html) broker written in Go based on gobroke by Junyi Luan. 
MQTT is an excellent protocal for mobile messaging. Facebook built the new Facebook Messager based on MQTT.

gobroke used a redis database connection for storage. This has been replaced with Bolt cross platform key/value database written in Go. 

#What is Gobroke for?#
A cross platform MQTT broker 


#Usage#
Super simple. Just run 

>go run gobroke 

The broker will start and listen on port 1883 for MQTT traffic. Command line flags:

* -p PORT: specify MQTT broker's port, default is 1883
* -b the bolt database file, default is bolt.db
* -d: when set comprehensive debugging info will be printed, this may significantly harm performance.

#Dependency#
* [Bolt](https://github.com/boltdb/bolt) database for storage
* [Seelog](https://github.com/cihub/seelog) for logging

#<a id="unsupported"></a>Not supported features#
* QOS level 2 is not supported. Only support QOS 1 and 0.

* Topic wildcard is not supported. Topic is always treated as plain string.
