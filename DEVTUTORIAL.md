<img src="http://i.imgur.com/RSUlFRa.gif" width="120" alt="HEP"><img src="https://d30y9cdsu7xlg0.cloudfront.net/png/30712-200.png" width=50>

# HEPop DEV Tutorial
This quick tutorial will allow testing the insertion pipeline, hep decoding and query formats using HEPop & RethinkDB

### Requirements
* RethinkDB 2.5.6
* [hepgen.js](http://github.com/sipcapture/hepgen.js)

#### Create Config
Save your preferences somewhere, ie: `/opt/hepop/myconfig.js` choosing either rethinkdb or postgres... or both!
```
{
   "id":"HEPop101",
   "socket":"udp",
   "port":2999,
   "address":"127.0.0.1",
   "queue":{
      "timeout":2000,
      "maxSize":1000,
      "useInterval":true
   },
   "dbName":"hepic",
   "tableName":"hep",
   "db":{
      "rethink":{
         "servers":[
            {
               "host":"rethinkdb.host",
               "port":28015
            }
         ]
      },
      "pgsql":{
         "host":"pgsql.host",
         "port":15432,
         "database":"hepic",
         "user":"postgres",
         "password":"sipcapture"
      }
   },
   "debug":true
}


```

#### Start HEPop
Launch HEPop using your configuration file. Launch with no parameters for `help`
```
nodejs hepop.js -c /opt/hepop/myconfig.js 
```

#### Send HEP data
```
cd hepgen.js && npm install
nodejs hepgen.js -s 127.0.0.1 -p 9060 -c ./config/scanner.js
```

#### RQL Query
Verify data
```
r.db('hepic').table('hep').filter({"rcinfo.payloadType": 1})
```

Create a secondary index for timestamps
```
r.db('hepic').table('hep').indexCreate("rcinfo.ts")
```

Search by Range:
```
r.db('hepic').table('hep').between( r.epochTime(1520684154), r.epochTime(1520684754), {index: 'rcinfo.ts'})
```
```
r.db('hepic').table('hep').between( new Date((new Date()).getTime() - 1000 * 60), new Date(), {index: 'rcinfo.ts'});
```

Search by Range, Method, select Payload:
```
r.db('hepic').table('hep').between( new Date((new Date()).getTime() - 10000 * 60), new Date(), {index: 'rcinfo.ts'}).filter({
    sip: {
        method: "INVITE"
    }
}).limit(10).getField('payload');
```

#### PGSql Query
Query the inserted data using JSON fields:
```
SELECT
 data -> 'rcinfo.payloadType' AS payloadType
FROM
 hepic;
 ```
