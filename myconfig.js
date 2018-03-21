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
  "db": {
	"rethink" : {
	  "servers":[
	    {
		"host": "127.0.0.1",
		"port":28015
	    }
  	  ]
	},
  	"pgsql":{
	  "url": "mongodb://localhost:27017/homer"
	},
  	"pgsql":{
  	  host: 'localhost', // 'localhost' is the default;
  	  port: 5432, // 5432 is the default;
  	  user: 'myUser',
  	  password: 'myPassword'
  	}
  },
  "debug": false
}
