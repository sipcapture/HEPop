# <img src="https://user-images.githubusercontent.com/1423657/55069501-8348c400-5084-11e9-9931-fefe0f9874a7.png" height=100/><img src="https://github.com/user-attachments/assets/c8e858ea-bb21-45f3-82ac-a1cd955f30cf" height=100>

**HEPop** is a prototype stand-alone [HEP](https://github.com/sipcapture/hep) Capture Server in Bun designed for [HOMER](https://github.com/sipcapture/homer)

> This is a work in progress. Do not use it!

##### Features

- [x] Bun Server
  - [x] hep-js sockets
  - [ ] parsip extractor
- [x] InfluxDB3/FlightSQL API
  - [x] Object Storage, Parquet
- [ ] HOMER Search API

```mermaid
%%{
  init: {
    'theme': 'base',
    'themeVariables': {
      'primaryColor': '#BB2528',
      'primaryTextColor': '#fff',
      'primaryBorderColor': '#7C0000',
      'lineColor': '#F8B229',
      'secondaryColor': '#006100',
      'tertiaryColor': '#fff'
    }
  }
}%%

  graph TD;
      HEP-Client-- hep -->server;
      HEPop:9069-- gRPC -->IOx:8181;
      server-->Storage;
      server-->Parquet-Compactor;
      Parquet-Compactor-->Storage;
      Storage-.->S3;
      Storage-.->Filesystem;

      subgraph server
        HEPop:9069
        IOx:8181
      end

```


### Install & Build

Use Bun to generate a bundled standalone binary for `hepop`

```bash
bun install
bun build ./hepop.js --compile --outfile hepop
```

### Run
Configure the client using ENV variables
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

### Ingestion
Just send hep to the server using UDP/TCP. Each HEP type will generate a table _(hep_1, hep_100, etc)_

### Query
Query the HEP data using the HTTP API or Flight SQL
#### API
```bash
# curl http://127.0.0.1:8181/api/v3/query_sql --data '{"db": "hep", "q": "select * from hep_1 limit 1"}'
```
```json
[{"capture_id":2001,"capture_pass":"myHep","correlation_id":"067d3@127.0.0.1","dst_ip":"192.168.1.2","dst_port":5060,"ip_family":2,"payload":"OPTIONS sip:127.0.0.1 SIP/2.0Call-ID: 067d3@127.0.0.1CSeq: 9999 OPTIONSFrom: <sip:nodejs@127.0.0.1>;tag=2628881569To: <sip:nodejs@127.0.0.1>Via: SIP/2.0/UDP 127.0.0.1:48495;branch=z9hG4bK9b82aa8fb4c7705466a3456dfff7f384333332Max-Forwards: 70User-Agent: HEPGEN-UACContent-Length: 0","proto_type":0,"protocol":17,"src_ip":"192.168.1.1","src_port":5060,"time":"2025-01-26T18:44:07.120","time_sec":1737917047,"time_usec":120000,"type":"1"}]
```
#### Flight
```sql
influxdb3 query --database hep "SELECT * FROM hep_1 limit 1"
+------------+--------------+-----------------+-------------+----------+-----------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------+----------+-------------+----------+-------------------------+------------+-----------+------+
| capture_id | capture_pass | correlation_id  | dst_ip      | dst_port | ip_family | payload                                                                                                                                                                                                                                                                                  | proto_type | protocol | src_ip      | src_port | time                    | time_sec   | time_usec | type |
+------------+--------------+-----------------+-------------+----------+-----------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------+----------+-------------+----------+-------------------------+------------+-----------+------+
| 2001       | myHep        | 067d3@127.0.0.1 | 192.168.1.2 | 5060     | 2         | OPTIONS sip:127.0.0.1 SIP/2.0Call-ID: 067d3@127.0.0.1CSeq: 9999 OPTIONSFrom: <sip:nodejs@127.0.0.1>;tag=2628881569To: <sip:nodejs@127.0.0.1>Via: SIP/2.0/UDP 127.0.0.1:48495;branch=z9hG4bK9b82aa8fb4c7705466a3456dfff7f384333332Max-Forwards: 70User-Agent: HEPGEN-UACContent-Length: 0 | 0          | 17       | 192.168.1.1 | 5060     | 2025-01-26T18:44:07.120 | 1737917047 | 120000    | 1    |
+------------+--------------+-----------------+-------------+----------+-----------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------+----------+-------------+----------+-------------------------+------------+-----------+------+
```
