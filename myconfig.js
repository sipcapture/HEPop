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
  	"pgsql":{
  	  "host": "localhost",
  	  "port": 5432,
  	  "user": "homer_user",
  	  "password": "homer_password"
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
  "db_off": { 
	"rethink" : {
	  "servers":[
	    { "host": "127.0.0.1", "port":28015 }
  	  ]
	},
  	"mongodb":{
	  "url": "mongodb://localhost:27017/homer"
	},
	"elastic" : {
     	  "target": "http://localhost:9200",
     	  "max_bulk_qtty": 1000,
     	  "max_request_num": 20, 
     	  "index": "hep"
        }
  },
  "debug": false
}
