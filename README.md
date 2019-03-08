# Unichem2index

Small tool for information replication between Unichem's DB and an ElasticSearch index.

## How to use

After installing oracle's oracle-instaclient on your machine run:
```bash
unichem2index -eshost="http://0.0.0.0:9200" -oraconn=hr/hr@localhost:1521:XE
```
### Flags
- **eshost** (Mandatory): ElasticSearch host, e.g.: ```-eshost="http://0.0.0.0:9200"```
- **oraconn** (Mandatory): Oracle DB connection string, e.g.: ```-oraconn=hr/hr@localhost:1521:XE```
- **v**: Software version
- **d**: Debug log level

> NOTE: Setting the log level to Debug will greatly decrease performance 

## How to build

A Go installation its required in order to compile Unichem2index, it can be downloaded from [here](https://golang.org/doc/install). 

```bash
make build
```

### Dockerized 

To run it on Docker its required to download [oracle's instant client](https://www.oracle.com/technetwork/topics/linuxx86-64soft-092277.html) files and place them on 
the Docker folder, e.g.:
```bash
Docker/oracle-instantclient18.3-basic-18.3.0.0.0-1.x86_64.rpm
Docker/oracle-instantclient18.3-devel-18.3.0.0.0-1.x86_64.rpm
Docker/oracle-instantclient18.3-sqlplus-18.3.0.0.0-1.x86_64.rpm
```

