

# <img src="https://d30y9cdsu7xlg0.cloudfront.net/png/30712-200.png" width=80><img src="https://user-images.githubusercontent.com/1423657/38167108-1797647a-3530-11e8-9cef-349459d8fa21.png" width=250>
NodeJS [HEP](https://hep.sipcapture.org) Server & Switch for [HOMER 7](https://github.com/sipcapture/homer) 

## WARNING
This is an **unstable prototype** under heavy development, please **do not use** for any purpose other than testing!

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

#### Screenshots
##### homer7
<img src="https://user-images.githubusercontent.com/1423657/38167363-37b30454-3534-11e8-9b18-e06564f3dd3a.png" width=500/>

##### influxdb
<img src="https://user-images.githubusercontent.com/1423657/38167092-d89ebeb2-352f-11e8-8a67-7ada2fa1967e.png" width=500/>

----

#### Made by Humans
This Open-Source project is made possible by actual Humans without corporate sponsors, angels or patreons.<br>
If you use this software in production, please consider supporting its development with contributions or [donations](https://www.paypal.com/cgi-bin/webscr?cmd=_donations&business=donation%40sipcapture%2eorg&lc=US&item_name=SIPCAPTURE&no_note=0&currency_code=EUR&bn=PP%2dDonationsBF%3abtn_donateCC_LG%2egif%3aNonHostedGuest)

[![Donate](https://www.paypalobjects.com/en_US/i/btn/btn_donateCC_LG.gif)](https://www.paypal.com/cgi-bin/webscr?cmd=_donations&business=donation%40sipcapture%2eorg&lc=US&item_name=SIPCAPTURE&no_note=0&currency_code=EUR&bn=PP%2dDonationsBF%3abtn_donateCC_LG%2egif%3aNonHostedGuest) 
