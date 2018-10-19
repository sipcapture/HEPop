{
  "id" : "HEPOP_ID",
  "socket": "HEPOP_PROTO", 
  "port": HEPOP_PORT,
  "address": "HEPOP_HOST",
  "queue": {
    "timeout": 2000, 
    "maxSize": 1000, 
    "useInterval": true 
  },
  "dbName": "PGSQL_DBNAME",
  "tableName": "PGSQL_TBNAME",
  "db": {
	"rawSize": 8000,
  	"pgsql":{
  	  "host": "PGSQL_HOST",
  	  "port": PGSQL_PORT,
  	  "user": "PGSQL_USER",
  	  "database": "PGSQL_DBNAME",
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
  "db_off": { 
	"rethink" : {
	  "servers":[
	    { "host": "RETHINKDB_HOST", "port":28015 }
  	  ]
	},
  	"mongodb":{
	  "url": "mongodb://MONGODB_HOST:27017/homer"
	},
	"elastic" : {
     	  "target": "http://ELASTIC_HOST:ELASTIC_PORT",
     	  "max_bulk_qtty": 1000,
     	  "max_request_num": 20, 
     	  "index": "hep"
       }
  },
  "debug": HEPOP_DEBUG
}
