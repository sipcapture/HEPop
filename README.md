<img src="http://i.imgur.com/RSUlFRa.gif" width="120" alt="HEP"><img src="https://d30y9cdsu7xlg0.cloudfront.net/png/30712-200.png" width=50>

# HEPop
NodeJS [HEP](https://hep.sipcapture.org) Server & Switch for [HOMER](https://github.com/sipcapture/homer) 

### Usage
```
  hepop [options] [command]
```

#### Options:
```
    -V, --version            output the version number
    -p, --port <number>      port to listen on
    -a, --address <address>  network address to listen on
    -h, --help               output usage information
```

#### Commands:
```
    http    start HTTP HEP server
    tcp     start TCP  HEP server
    udp     start UDP  HEP server
```

#### Example
```
$ hepop -p 9063 udp

[08:25:40 PM] ▶️ UDP4 127.0.0.1 9063
[08:25:49 PM] ✔ [{"address":"127.0.0.1","family":"IPv4","port":57152,"size":843}] {"protocolFamily":2,"protocol":17,"srcIp":"192.168.1.1","dstIp":"192.168.1.2","srcPort":5060,"dstPort":5060,"timeSeconds":1517772349,"timeUseconds":814,"payloadType":1,"captureId":2001,"capturePass":"myHep","payload":"INVITE sip:900442037690000@someprovider.com SIP/2.0\nTo: 900442037690000<sip:900442037690000@someprovider.com>\nFrom: 1<sip:1@192.168.99.99>;tag=2fd7b751\nVia: SIP/2.0/UDP 192.168.99.99:5071;branch=z9hG4bK-897b2a55c16140a97dab4273ac879fb0;rport\nCall-ID: tm70x@127.0.0.1\nCSeq: 1 INVITE\nContact: <sip:1@10.0.10.1:5071>\nMax-Forwards: 70\nAllow: INVITE, ACK, CANCEL, BYE\nUser-Agent: sipcli/v1.8\nContent-Type: application/sdp\nContent-Length: 283\n\nv=0\no=sipcli-Session 1189727098 1654538214 IN IP4 192.168.99.99\ns=sipcli\nc=IN IP4 192.168.99.99mt=0 0\nm=audio 5073 RTP/AVP 18 0 8 101\na=fmtp:101 0-15\na=rtpmap:18 G729/8000\na=rtpmap:0 PCMU/8000\na=rtpmap:8 PCMA/8000\na=rtpmap:101 telephone-event/8000\na=ptime:20\na=sendrecv\n\r\n\r\n"}
```
