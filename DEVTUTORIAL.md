<img src="http://i.imgur.com/RSUlFRa.gif" width="120" alt="HEP"><img src="https://d30y9cdsu7xlg0.cloudfront.net/png/30712-200.png" width=50>

# HEPop DEV Tutorial
This quick tutorial will allow testing the insertion pipeline, hep decoding and query formats using HEPop & RethinkDB

### Requirements
* RethinkDB 2.5.6
* [hepgen.js](http://github.com/sipcapture/hepgen.js)

#### Create Config
```
{
  "id" : "HEPop101",
  "dbName": "hepic",
  "tableName": "hep",
  "rethink" : {
	"servers":[
		{
		  "host": "your.rethinkdb.server",
		  "port":28015
		}
	]
  }
}

```

#### Start HEPop

```
nodejs hepop.js -p 9222 udp -c /opt/hepop/myconfig.js 
```

#### Send HEP data
```
cd hepgen.js && npm install
nodejs hepgen.js -p 9222 -c ./config/scanner.js
```

#### RQL Query
Verify data
```
r.db('hepic').table('hep').filter({"rcinfo.payloadType": 1})
```
```
{
"id":  "98dc95d4-2810-4753-9b01-8953c1103294" ,
"payload":  "INVITE sip:900442037690000@someprovider.com SIP/2.0
To: 900442037690000<sip:900442037690000@someprovider.com>
From: 1<sip:1@192.168.99.99>;tag=2fd7b751
Via: SIP/2.0/UDP 192.168.99.99:5071;branch=z9hG4bK-897b2a55c16140a97dab4273ac879fb0;rport
Call-ID: 6snx7@127.0.0.1
CSeq: 1 INVITE
Contact: <sip:1@10.0.10.1:5071>
Max-Forwards: 70
Allow: INVITE, ACK, CANCEL, BYE
User-Agent: sipcli/v1.8
Content-Type: application/sdp
Content-Length: 283

v=0
o=sipcli-Session 1189727098 1654538214 IN IP4 192.168.99.99
s=sipcli
c=IN IP4 192.168.99.99mt=0 0
m=audio 5073 RTP/AVP 18 0 8 101
a=fmtp:101 0-15
a=rtpmap:18 G729/8000
a=rtpmap:0 PCMU/8000
a=rtpmap:8 PCMA/8000
a=rtpmap:101 telephone-event/8000
a=ptime:20
a=sendrecv



" ,
"rcinfo.captureId": 2001 ,
"rcinfo.capturePass":  "myHep" ,
"rcinfo.dstIp":  "192.168.1.2" ,
"rcinfo.dstPort": 5060 ,
"rcinfo.payloadType": 1 ,
"rcinfo.protocol": 17 ,
"rcinfo.protocolFamily": 2 ,
"rcinfo.srcIp":  "192.168.1.1" ,
"rcinfo.srcPort": 5060 ,
"rcinfo.timeSeconds": 1520684754 ,
"rcinfo.timeUseconds": 456 ,
"rcinfo.ts": Sat Mar 10 2018 12:25:54 GMT+00:00
}
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

