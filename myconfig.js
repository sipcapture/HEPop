{
  "id" : "HEPop101",
  "socket": "udp", // udp, tcp, http, sipfix
  "port": 9060,
  "address": "127.0.0.1",
  "queue": {
    "timeout": 2000, //if there's no data input until timeout, emit data forcefully.
    "maxSize": 1000, //data emitted count
    "useInterval": true //if this value is true, data event is emitted even If new data is pushed.
  },
  "dbName": "hepic",
  "tableName": "hep",
  "rethink" : {
    "servers":[
	{
	  "host": "127.0.0.1",
	  "port":28015
	}
    ]
  },
  "debug": false
}
