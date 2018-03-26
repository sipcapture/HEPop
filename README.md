<img src="http://i.imgur.com/RSUlFRa.gif" width="120" alt="HEP"><img src="https://d30y9cdsu7xlg0.cloudfront.net/png/30712-200.png" width=50>

# HEPop
NodeJS [HEP](https://hep.sipcapture.org) Server & Switch for [HOMER](https://github.com/sipcapture/homer) 

## WARNING
This is a prototype under heavy development, please do not use for any purpose other than testing!

### About

*HEPop* is a pure NodeJS Capture Server featuring native HEP3 decoding and experimental backend support for:
  * PGSQL
  * MongoDB
  * RethinkDB
  * Elasticsearch



### Usage
```
  hepop [options]
```

#### Options:
```
    -V, --version                  output the version number
    -p, --port <number>            port to listen on (default: 9060)
    -a, --address <address>        network address to listen on
    -d, --dbName <address>         database name
    -t, --tableName <address>      database table name
    -c, --configfile <configfile>  configuration file
    -s, --socket <socket>          socket service (udp,tcp,http,sipfix) (default: udp)
```
#### Config
See [myconfig.js](https://github.com/sipcapture/HEPop/blob/master/myconfig.js) for an example configuration bundle

#### Socket Types:
```
    http    start HTTP HEP server
    tcp     start TCP  HEP server
    udp     start UDP  HEP server
```

#### Examples 
##### Using Options
```
$ hepop -p 9060 -s udp

[08:25:40 PM] ▶️ Initializing Bulk bucket...
[08:25:40 PM] ▶️ UDP4 127.0.0.1 9060
[08:25:49 PM] ✔ [{"address":"127.0.0.1","family":"IPv4","port":57152,"size":843}] {"protocolFamily":2,"protocol":17,"srcIp":"192.168.1.1","dstIp":"192.168.1.2","srcPort":5060,"dstPort":5060,"timeSeconds":1517772349,"timeUseconds":814,"payloadType":1,"captureId":2001,"capturePass":"myHep","payload":"INVITE sip:900442037690000@someprovider.com SIP/2.0\nTo: 900442037690000<sip:900442037690000@someprovider.com>\nFrom: 1<sip:1@192.168.99.99>;tag=2fd7b751\nVia: SIP/2.0/UDP 192.168.99.99:5071;branch=z9hG4bK-897b2a55c16140a97dab4273ac879fb0;rport\nCall-ID: tm70x@127.0.0.1\nCSeq: 1 INVITE\nContact: <sip:1@10.0.10.1:5071>\nMax-Forwards: 70\nAllow: INVITE, ACK, CANCEL, BYE\nUser-Agent: sipcli/v1.8\nContent-Type: application/sdp\nContent-Length: 283\n\nv=0\no=sipcli-Session 1189727098 1654538214 IN IP4 192.168.99.99\ns=sipcli\nc=IN IP4 192.168.99.99mt=0 0\nm=audio 5073 RTP/AVP 18 0 8 101\na=fmtp:101 0-15\na=rtpmap:18 G729/8000\na=rtpmap:0 PCMU/8000\na=rtpmap:8 PCMA/8000\na=rtpmap:101 telephone-event/8000\na=ptime:20\na=sendrecv\n\r\n\r\n"}
```
##### Using Configuration file
```
$ hepop --configfile ./myconfig.js udp

[02:44:41 AM] ▶️ Creating a pool connected to 10.0.0.1:28015
[02:44:41 AM] ▶️ Initializing Bulk bucket...
[02:44:41 AM] ▶️ UDP4 127.0.0.1 9063
[02:44:42 AM] ✔ [{"address":"127.0.0.1","family":"IPv4","port":57152,"size":843}] {"protocolFamily":2,"protocol":17,"srcIp":"192.168.1.1","dstIp":"192.168.1.2","srcPort":5060,"dstPort":5060,"timeSeconds":1517772349,"timeUseconds":814,"payloadType":1,"captureId":2001,"capturePass":"myHep","payload":"INVITE sip:900442037690000@someprovider.com SIP/2.0\nTo: 900442037690000<sip:900442037690000@someprovider.com>\nFrom: 1<sip:1@192.168.99.99>;tag=2fd7b751\nVia: SIP/2.0/UDP 192.168.99.99:5071;branch=z9hG4bK-897b2a55c16140a97dab4273ac879fb0;rport\nCall-ID: tm70x@127.0.0.1\nCSeq: 1 INVITE\nContact: <sip:1@10.0.10.1:5071>\nMax-Forwards: 70\nAllow: INVITE, ACK, CANCEL, BYE\nUser-Agent: sipcli/v1.8\nContent-Type: application/sdp\nContent-Length: 283\n\nv=0\no=sipcli-Session 1189727098 1654538214 IN IP4 192.168.99.99\ns=sipcli\nc=IN IP4 192.168.99.99mt=0 0\nm=audio 5073 RTP/AVP 18 0 8 101\na=fmtp:101 0-15\na=rtpmap:18 G729/8000\na=rtpmap:0 PCMU/8000\na=rtpmap:8 PCMA/8000\na=rtpmap:101 telephone-event/8000\na=ptime:20\na=sendrecv\n\r\n\r\n"}
```

----

#### Made by Humans
This Open-Source project is made possible by actual Humans without corporate sponsors, angels or patreons.<br>
If you use this software in production, please consider supporting its development with contributions or [donations](https://www.paypal.com/cgi-bin/webscr?cmd=_donations&business=donation%40sipcapture%2eorg&lc=US&item_name=SIPCAPTURE&no_note=0&currency_code=EUR&bn=PP%2dDonationsBF%3abtn_donateCC_LG%2egif%3aNonHostedGuest)

[![Donate](https://www.paypalobjects.com/en_US/i/btn/btn_donateCC_LG.gif)](https://www.paypal.com/cgi-bin/webscr?cmd=_donations&business=donation%40sipcapture%2eorg&lc=US&item_name=SIPCAPTURE&no_note=0&currency_code=EUR&bn=PP%2dDonationsBF%3abtn_donateCC_LG%2egif%3aNonHostedGuest) 
