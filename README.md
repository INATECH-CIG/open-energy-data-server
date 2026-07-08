<!--
SPDX-FileCopyrightText: Florian Maurer, Christian Rieke

SPDX-License-Identifier: AGPL-3.0-or-later
-->

# Open Energy Data Server

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.10607894.svg)](https://doi.org/10.5281/zenodo.10607894)

This repository is to create an institute-wide available database that can be set up once and then be used by multiple researchers.

Allowing native access through PostgreSQL allows any easy integration of different software which can access data from a SQL database.

For an interactive Documentation, please visit the [Read the Docs Page](https://open-energy-data-server.readthedocs.io/en/latest/index.html).

![Basic outline of the architecture and included services](docs/source/media/oeds-architecture.png)


## Getting started

To set up your institutes new open-data server, you can [install docker](https://docs.docker.com/engine/install/) or [podman](https://podman.io/).
Then do `cp .env_template .env` and `nano .env` to set credentials. 

We also need to create the directory for postgres data storage and give postgres permission, which is done by  
`mkdir -p ./data/open-data-ha`  
`sudo chown -R 1000:1000 ./data/open-data-ha`  
`sudo chmod 700 ./data/open-data-ha`  

Open the file `/data/init.sql` and replace `READONLY_PW` with your password that matches the `READONLY_PW` from for `.env`

Start the `compose.yml` with `docker compose up -d`.

Then you have a running TimescaleDB server listening on postgresql default port `5432`.

![Visualization of OEDS Usage Workflow](docs/source/media/oeds-workflow.png)

As seen in the above workflow outline, the data is inserted by scripts which retrieve the data from a source API.
This is the core part, afterwards, everything is basically usable.

You can install all python dependencies:

`pip install -r requirements.txt`

Furthermore, you need to copy the `config.example.yml` to `config.yml` and adjust the credentials for access.

And finally run the main crawling script `python main.py` to download all available sources into the database.

## Prefect

This project uses [prefect](https://docs.prefect.io/v3/get-started) for orchestrating all the workflows. 
Prefect can automatically run python scripts e.g. for crawling, processing and analysing data. 
An overview dashboard of your flows and runs is shown at http://<ip>:4200/runs).

### Deploying flows

To add a script and automate it with prefect, you have to deploy it.
There are many ways to deploy your script so that it gets executed by prefect. We suggest the following workflow which can and should be done from your local machine: 

- Make sure the script you want to automate has a method which is decorated with `@flow` and is connected to a github repo. 
- Make sure you have installed the prefect CLI tool. 
- In the root directory of your repo, create the file `prefect.yaml` with the following structure: 
```
deployments:
  - name: <name of deployment>
    entrypoint: <path to your script from repo root:flow method>
    work_pool:
      name: local-pool
    pull:
      - prefect.deployments.steps.git_clone:
          id: clone-step
          repository: <repo of your code>
          branch: <the branch to pull from(usually main)>
      - prefect.deployments.steps.pip_install_requirements:
      directory: "{{ clone-step.directory }}"
      requirements_file: requirements.txt
  ```

- On your local machine, run `prefect config set PREFECT_API_URL=http://<server-ip>:<prefect api port (usually 4200)>/api `
- Run `prefect work-pool ls` and check if the `local-pool` id is the same as when you run it on the oeds data server.
- Run `prefect deploy -n <deployment name>`. You will be able to initiate schedules.
- Verify that the deployment and the runs are present under `http://<server-ip>:<prefect api port (usually 4200)>`

See [here](https://docs.prefect.io/v3/how-to-guides/deployments/create-deployments) for more information about deploying. 


## Metabase

To access, browse and download the data, we provide an metabase instance. Once the metabase container started,
the admin should go to http://<ip>:3000 to claim the admin account. After this you can connect the database 
with metabase using the connection string `postgresql://readonly:<READONLY_PW>@open-data-17:<DB_PORT>/opendata` .
You can verify the connection by clicking on `Data` > `Databases` > `opendata` > `public` . Once you have uploaded data into the db, all tables
should appear here. you can click on any table, browse and download the data (bottom right corner).

### Users, Groups and Data Access

As an admin click `⚙`️ > `Admin setting`. 
Click on `People` to invite someone to metabase. At the bottom you can select the group for this user. 
The group defines which data can be seen by an user. 

To create a new group, click on `Groups` (left side of the `People` menu) and create a new group. Click on the new group to add users. 

To manage permissions, click on `Permissions` , no select what the user may see (always choose granular to step down a level in granularity).

### Change column/table names an descriptions

Metabase changes column and table names to be 'more human readable'. This also removes the country codes in Caps and makes them mixed case. Unfortunately there is no way to turn this off completely, but you give new display names for each column (which don't change anything in the real database). To do this, login as an Admin and click `⚙`️ > `Admin setting` > `Table Metadata` > `opendata` > `<corresponding schema>` > `<corresponding table>` . Here you can change the displayed name of the table and each column/field and also add descriptions to it. 

## TimeScaleDB

The used database technology for the database server is [TimescaleDB](https://timescale.com/) which is an extension for PostgreSQL (just like PostGIS but for timeseries databases).

### What is a time-series database?
Normal SQL tables can get quite slow if millions of entries are stored in them.

Luckily, timeseries data has the property of always having a separation at the time column.
This can be used for sharding of the database table.

Popular systems like InfluxDB are using this to improve queries with data aggregation or long-time history analysis.
Unfortunately, such databases do not allow storing data without a time column.
For example metadata or lists of existing power plants.

To be able to use both, TimeScaleDB seemed to be the best candidate.
The Grafana integration works also very well and clients can work with it, just like with every PostgreSQL server, without having a new query language to learn (like Flux for example).

### Replication
TimescaleDB allows having replication across multiple servers for load balancing and improvements for reading (and sometimes writing) timeseries data.
This works by using [Distributed Hypertables](https://docs.timescale.com/timescaledb/latest/how-to-guides/distributed-hypertables).

On a high level this can be imagined that for a query spanning a year, each of the three nodes calculates and aggregates the query result for 4 months - resulting in a higher performance.
This only works for timeseries tables and is not compatible with non-timeseries data.
Therefore to increase replication of other tables (like the Marktstammdatenregister), one still needs to have manual replication or use something like [Patroni](https://patroni.readthedocs.io/en/latest/).

## Contributing

Do you know of other interesting open-access databases which are worth mentioning here?
Maybe some are too volatile, large or unknown and are therefore not useful to store in the [OEP](https://openenergy-platform.org/).

Just send a PR and add a new file in the crawler folder with your implemented `ContinuousCrawler` or `DownloadOnceCrawler` and add it to the repository.

## Citation

You can cite the `open-energy-data-server` through the Conference proceedings:

> Maurer, F., Sejdija, J., & Sander, V. (2024, February 2). Decentralized energy data storages through an Open Energy Database Server. 1st NFDI4Energy Conference (NFDI4Energy), Hanover, Germany. https://doi.org/10.5281/zenodo.10607895
