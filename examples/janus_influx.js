{
  "id" : "HEPop101",
  "socket": "http",
  "port": 8080,
  "address": "0.0.0.0",
  "queue": {
    "timeout": 2000,
    "maxSize": 1000,
    "useInterval": true
  },
  "dbName": "homer_data",
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
  "debug": false
}
