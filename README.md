

# <img src="https://user-images.githubusercontent.com/1423657/55069501-8348c400-5084-11e9-9931-fefe0f9874a7.png" width=200/><img src="https://user-images.githubusercontent.com/1423657/38167420-070b1a0c-3535-11e8-8d25-be0f38779b7b.png"/>

**HEPop** is a prototype stand-alone [HEP](https://github.com/sipcapture/hep) Capture Server designed for [HOMER7](https://github.com/sipcapture/homer) capable of emitting indexed datasets and tagged timeseries to multiple backends in bulks. HEPop is developed using `NodeJS` and distributed using `npm`.

*TLDR; instant, stand-alone, minimal HOMER Server without Kamailio or OpenSIPS dependency/options.*


## WARNING
This is a **prototype** under heavy development, please **use it with caution** and kindly report bugs!

### Features

The core of `hepop` follows the latest `homer` data design and splits indexed data and timeseries, providing multiple backend options to choose from when prototype a custom capture solution.

### Performance
During testing, HEPop bulking clocked about 10-15k/PPS per shared core on a Xeon(R) CPU E5-2660 v3 @ 2.60GHz

#### Supported Stores
| TYPE       |               |
|------------|-----------------|
| JSON       | Postgres (default), RethinkDB, MongoDB, Elasticsearch |
| Timeseries | InfluxDB (default), Prometheus, VictoriaMetrics |

#### Supported Sources

| SOCKET    | PROTO      | TYPE/ID         | DESCRIPTION        | STORE      | STATS                        |
|-----------|------------|-----------------|--------------------|------------|------------------------------|
| HEP       | HEP        | 1               | SIP                | JSON + TS  | SIP methods, SIP responses   |
|           |            | 5               | RTCP Reports       | Timeseries | RTCP, RTCPXR, X-RTP-Stat     |
|           |            | 34              | RTP Agent Report   | Timeseries | RTP, RTCP Stats              |
|           |            | 100             | JSON Logs          | JSON       | String, JSON Object          |
|           |            | 1000+           | Dynamic Types      | JSON       |                              |
| HTTP/S    | JANUS      | any             | Custom JSON Fields | JSON       | session_id, handle_id, opaque_id, event |
|           |            | 34              | Media Stats        | Timeseries | RTCP Statistics              |
| HTTP/S    | MEDIASOUP  | any             | Custom JSON Fields | JSON       | roomId, peerName, producerId |
|           |            | producer.stats  | Media Stats        | Timeseries | RTCP Statistics              |
|           |            | transport.stats | Transport Stats    | Timeseries | IP SRC/DST, Bytes in/out     |
| HTTPS     | JITSI      | any             | Custom JSON Fields | JSON       | report counters              |
|           |            | rtp.stats       | Browser Media Stats| Timeseries | RTCP Statistics              |
| SIPFIX    |            | SIP (tcp/udp)   | SIP comm-monitor   | JSON + TS  | SIP methods, SIP responses   |
|           |            | RTP QOS (stats) | RTP Media stats    | Timeseries | RTP, RTCP Stats              |



### Usage
Launch a dedicated instance of `hepop` per protocol using a custom configuration:
```
$ hepop -c ./myconfig.js
```
#### Docker
Try the bundled [Docker compose](https://github.com/sipcapture/homer-docker/tree/master/hepop/hom7-hep-influx)

#### Config
See [myconfig.js](https://github.com/sipcapture/HEPop/blob/master/myconfig.js) for an example configuration

#### Options:
```
    -V, --version                  output the version number
    -c, --configfile <configfile>  configuration file
    -s, --socket <socket>          socket service (udp,tcp,http,sipfix,mqtt) (default: udp)
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
