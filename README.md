
# <img src="https://user-images.githubusercontent.com/1423657/38167420-070b1a0c-3535-11e8-8d25-be0f38779b7b.png"/>

**HEPop** is a stand-alone [HEP](https://github.com/sipcapture/hep) Capture Server developed in NodeJS, designed to prototype different backends for [HOMER7](https://github.com/sipcapture/homer) and emitting Metrics to external backends such as InfluxDB and Prometheus.

*TLDR; instant, stand-alone, minimal HOMER Server without Kamailio or OpenSIPS dependency/options.*


## WARNING
This is an **unstable prototype** under heavy development, please **do not use** for any purpose other than testing!

### Features

- [x] HEP
  - TYPES
    - [x] 1: SIP, RTCP-XR, X-RTP, P-RTP-Stats
    - [x] 5: RTCP Reports
    - [x] 34: RTPAGent Reports
    - [x] 100: JSON Logs
- [x] Storage
  - SQL
    - [x] PGSQL (JSON)
  - NOSQL
    - [x] MongoDB
    - [x] RethinkDB
    - [x] Elasticsearch
- [x] Metrics
  - [x] InfluxDB
  - [ ] Prometheus


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
See [myconfig.js](https://github.com/sipcapture/HEPop/blob/master/myconfig.js) for an example configuration


##### Using Configuration file
```
$ hepop -c ./myconfig.js
```

#### Screenshots
##### homer7
<img src="https://user-images.githubusercontent.com/1423657/38173155-4f88f73e-35b9-11e8-86e1-d1d2e3013759.png" width=500/>

##### influxdb
<img src="https://user-images.githubusercontent.com/1423657/38167092-d89ebeb2-352f-11e8-8a67-7ada2fa1967e.png" width=500/>

----

#### Made by Humans
This Open-Source project is made possible by actual Humans without corporate sponsors, angels or patreons.<br>
If you use this software in production, please consider supporting its development with contributions or [donations](https://www.paypal.com/cgi-bin/webscr?cmd=_donations&business=donation%40sipcapture%2eorg&lc=US&item_name=SIPCAPTURE&no_note=0&currency_code=EUR&bn=PP%2dDonationsBF%3abtn_donateCC_LG%2egif%3aNonHostedGuest)

[![Donate](https://www.paypalobjects.com/en_US/i/btn/btn_donateCC_LG.gif)](https://www.paypal.com/cgi-bin/webscr?cmd=_donations&business=donation%40sipcapture%2eorg&lc=US&item_name=SIPCAPTURE&no_note=0&currency_code=EUR&bn=PP%2dDonationsBF%3abtn_donateCC_LG%2egif%3aNonHostedGuest) 
