{
  "id" : "HEPOP_ID",
  "socket": "HEPOP_PROTO", // udp, tcp, http, sipfix
  "port": HEPOP_PORT,
  "address": "HEPOP_HOST",
  "queue": {
    "timeout": 2000, // if there's no data input until timeout, emit data forcefully.
    "maxSize": 1000, // data emitted count
    "useInterval": true // enforce timeout expiration for sending
  },
  "dbName": "PGSQL_DBNAME",
  "tableName": "PGSQL_TBNAME",
  "db": {
	"rawSize": 8000,
  	"pgsql":{
  	  "host": "PGSQL_HOST",
  	  "port": PGSQL_HOST,
  	  "user": "PGSQL_USER",
  	  "password": "PGSQL_PASSWORD"
  	}
  },
  "metrics": {
	"influx":{
		"period": 30000,
		"expire": 300000, 
		"dbName": "INFLUXDB_DB",
		"hostname": "INFLUXDB_HOST:INFLUXDB_PORT"
	}
  },
  "db_off": { // disabled  backends for reference
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
  "debug": HEPOP_DEBUG
}
