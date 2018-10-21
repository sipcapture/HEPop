{
  "id" : "HEPop101",
  "socket": "udp", 
  "port": 9060,
  "address": "127.0.0.1",
  "queue": {
    "timeout": 2000, 
    "maxSize": 1000, 
    "useInterval": true 
  },
  "dbName": "hepic",
  "tableName": "hep",
  "db": {
	"rawSize": 8000,
  	"rethink" : {
	  "servers":[
	    { "host": "127.0.0.1", "port":28015 }
  	  ]
	}
  },
  "metrics": {
	"influx":{
		"period": 30000,
		"expire": 300000,
		"dbName": "homer",
		"hostname": "localhost:8086"
	}
  },
  "debug": false
}
