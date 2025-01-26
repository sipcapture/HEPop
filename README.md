# <img src="https://user-images.githubusercontent.com/1423657/55069501-8348c400-5084-11e9-9931-fefe0f9874a7.png" width=100/>

**HEPop** is a prototype stand-alone [HEP](https://github.com/sipcapture/hep) Capture Server in Bun designed for [HOMER](https://github.com/sipcapture/homer)

> This is a work in progress. Do not use it!

##### Features

- Bun Server
- InfluxDB3/FlightSQL API
- Object Storage, Parquet



### Install & Build
```bash
bun install
bun build ./hepop.js --compile --outfile hepop
```

### Run
Configure the client using ENV
```
  INFLUX_HOST: "http://influxdb3:8181"
  INFLUX_TOKEN: "optional"
  INFLUX_DATABASE: "hep"
```
Run the HEP Server
```bash
./hepop
```

## Example
The repository includes a stand-alone example using hepop and influxdb3 with file storage
```
docker compose up
```
