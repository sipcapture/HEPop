<img src="http://i.imgur.com/RSUlFRa.gif" width="120" alt="HEP"><img src="https://d30y9cdsu7xlg0.cloudfront.net/png/30712-200.png" width=50>

# HEPop
NodeJS [HEP](https://hep.sipcapture.org) Server & Switch for [HOMER 7](https://github.com/sipcapture/homer) 

## WARNING
This is an unstable prototype under heavy development, please do not use for any purpose other than testing!

### About

*HEPop* is a pure NodeJS Capture Server featuring native HEP3 decoding, bulking and experimental backend support

- [x] Data
  - SQL
    - [x] PGSQL
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

----

#### Made by Humans
This Open-Source project is made possible by actual Humans without corporate sponsors, angels or patreons.<br>
If you use this software in production, please consider supporting its development with contributions or [donations](https://www.paypal.com/cgi-bin/webscr?cmd=_donations&business=donation%40sipcapture%2eorg&lc=US&item_name=SIPCAPTURE&no_note=0&currency_code=EUR&bn=PP%2dDonationsBF%3abtn_donateCC_LG%2egif%3aNonHostedGuest)

[![Donate](https://www.paypalobjects.com/en_US/i/btn/btn_donateCC_LG.gif)](https://www.paypal.com/cgi-bin/webscr?cmd=_donations&business=donation%40sipcapture%2eorg&lc=US&item_name=SIPCAPTURE&no_note=0&currency_code=EUR&bn=PP%2dDonationsBF%3abtn_donateCC_LG%2egif%3aNonHostedGuest) 
