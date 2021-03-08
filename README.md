# Unichem2index

Small tool for information replication between Unichem's DB and an ElasticSearch index.

## How to use

After installing oracle's oracle-instaclient on your machine run:
```bash
unichem2index -eshost="http://0.0.0.0:9200" -oraconn=hr/hr@localhost:1521:XE
```

### Configuration File

```
# Oracle Connection String for Unichems DB
oracleconn: 'hr/hr@oradb:1521/xe'

# ElasticSearch host, index and type
elastichost: 'http://elasticsearch:9200'
index: Unichem
type: Compound

# Query parameters for extraction
querystart: 2 # Starting number, useful to resume stoped extractions
querylimit: 5 # Amount of records fetch on each query
# Elastic Bulk add parameters
bulklimit: 2 # Limit of records to add for each Bulk worker
maxbulkcalls: 4 # Maximum number of worker at one time

logpath: ''
```

### Flags

- **eshost** (Mandatory): ElasticSearch host, e.g.: ```-eshost="http://0.0.0.0:9200"```. This will override the ES host on the config file.
- **oraconn** (Mandatory): Oracle DB connection string, e.g.: ```-oraconn=hr/hr@localhost:1521:XE```. This will override the Oracle Connection Strin on the config file (oraclenconn).
- **v**: Software version
- **d**: Debug log level

> NOTE: Setting the log level to Debug will greatly decrease performance 

## How to build

A Go installation its required in order to compile Unichem2index, it can be downloaded from [here](https://golang.org/doc/install). 

```bash
make build
```

1. Build using Docker Compose  ```docker-compose up golangBuilder```
2. Build using Docker Compose  ```docker-compose build unichem```
3. docker push chembl/unichem2index:latest

### Dockerized 

To run it on Docker its required to download [oracle's instant client](https://www.oracle.com/technetwork/topics/linuxx86-64soft-092277.html) files and place them on 
the Docker folder, e.g.:
```bash
Docker/oracle-instantclient18.3-basic-18.3.0.0.0-1.x86_64.rpm
Docker/oracle-instantclient18.3-devel-18.3.0.0.0-1.x86_64.rpm
Docker/oracle-instantclient18.3-sqlplus-18.3.0.0.0-1.x86_64.rpm
```
Make sure to place your ```config.yaml``` file inside the Docker folder as well. [config.yaml example](config.yaml.example)
