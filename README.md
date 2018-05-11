
# <img src="https://user-images.githubusercontent.com/1423657/38167420-070b1a0c-3535-11e8-8d25-be0f38779b7b.png"/>

**HEPop** is a prototype stand-alone [HEP](https://github.com/sipcapture/hep) Capture Server designed for [HOMER7](https://github.com/sipcapture/homer) capable of emitting indexed datasets and tagged timeseries to multiple backends in bulks. HEPop is developed using `NodeJS` and distributed using `npm`.

*TLDR; instant, stand-alone, minimal HOMER Server without Kamailio or OpenSIPS dependency/options.*


## WARNING
This is a **prototype** under heavy development, please **use it with caution** and kindly report bugs!

### Features

| PROTO     | ID              | DESCRIPTION        | STORE      | STATS                        |
|-----------|-----------------|--------------------|------------|------------------------------|
| HEP       | 1               | SIP                | JSON       | SIP methods, SIP responses   |
|           | 5               | RTCP Reports       | Timeseries | RTCP, RTCPXR, X-RTP-Stat     |
|           | 34              | RTP Agent Report   | Timeseries | RTP, RTCP Stats              |
|           | 100             | JSON Logs          | JSON       | String, JSON Object          |
|           | 1000+           | Dynamic Types      | JSON       |                              |
| JANUS     | any             | Custom JSON Fields | JSON       | session_id, handle_id, opaque_id, event |
|           | 34              | Media Stats        | Timeseries | RTCP Statistics              |
| MEDIASOUP | any             | Custom JSON Fields | JSON       | roomId, peerName, producerId |
|           | producer.stats  | Media Stats        | Timeseries | RTCP Statistics              |
|           | transport.stats | Transport Stats    | Timeseries | IP SRC/DST, Bytes in/out     |
|           |                 |                    |            |                              |

#### Supported Stores
| TYPE       |               |
|------------|-----------------|
| JSON       | Postgres, RethinkDB, MongoDB, Elasticsearch |
| Timeseries | InfluxDB, Prometheus |

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
