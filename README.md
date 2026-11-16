# MovieLens Big Data Analytics with Dockerized Hadoop + Spark

This repository preserves the existing MovieLens analytics workflow and extends it with a Docker-based three-node Hadoop and Apache Spark cluster. The project keeps the current Streamlit dashboard, processed outputs, and raw dataset while adding a containerized distributed environment for HDFS and Spark.

## Project goals

- Keep the existing analytics logic and dashboard working without rebuilding the application from scratch.
- Run a three-container Hadoop and Spark cluster locally on one laptop through Docker Compose.
- Use a reusable image for all nodes with role-specific startup scripts.
- Keep HDFS metadata and DataNode storage persistent across container restarts.
- Provide a simple path to run the MovieLens analytics job against HDFS and visualize results with Streamlit.

## Current implementation

The repository now contains:

- a three-node Docker Compose cluster named `master`, `worker1`, and `worker2`
- Hadoop HDFS configured for a NameNode + two DataNodes
- Apache Spark configured with a master on `master` and workers on `worker1` and `worker2`
- a reusable Docker image based on Python 3.12 and Java 17
- persistent Docker volumes for NameNode and DataNode storage
- a dedicated bridge network for internal cluster communication

## Architecture

The cluster is a simulated three-node environment running in Docker on a single physical machine. This is not three physical PCs; it is a multi-container distributed topology created inside one host environment.

```text
+-------------------------------+
| Docker Host / WSL2 Ubuntu     |
|                               |
|  master                       |
|  - HDFS NameNode              |
|  - Spark Master               |
|  - PySpark job submission     |
|  + ports: 9000, 9870, 7077, 8080 |
|                               |
|  worker1                      |
|  - HDFS DataNode              |
|  - Spark Worker               |
|                               |
|  worker2                      |
|  - HDFS DataNode              |
|  - Spark Worker               |
+-------------------------------+
```

Container-to-container communication uses the Docker Compose service names:

- `master`
- `worker1`
- `worker2`

The HDFS default filesystem is configured as:

- `hdfs://master:9000`

The Spark master URL is configured as:

- `spark://master:7077`

## Files in this repo

```text
MovieLensProject/
├── dashboard/
│   └── app.py
├── src/
│   └── ratings_analysis.py
├── data/
│   └── processed/
├── ml-latest-small/
├── config/
│   ├── core-site.xml
│   ├── hdfs-site.xml
│   └── workers
├── docker/
│   ├── Dockerfile
│   ├── master-entrypoint.sh
│   └── worker-entrypoint.sh
├── docker-compose.yml
├── requirements.txt
├── README.md
├── .venv/
├── hadoop-3.4.2/
├── ml-latest-small.zip
└── .gitignore
```

## Existing project components preserved

The project continues to use:

- the existing MovieLens raw dataset in `ml-latest-small/`
- the existing Spark ETL and analytics job in `src/ratings_analysis.py`
- the existing Streamlit dashboard in `dashboard/app.py`
- the existing processed analytics CSV outputs in `data/processed/`

The repo keeps these files intact and adds Docker infrastructure around them without replacing the original application logic.

## Requirements and versions

The implementation is aligned to the following versions and compatibility assumptions:

- Java 17
- Hadoop 3.4.2
- Apache Spark 4.3.0
- Python 3.12
- Docker Desktop + WSL2 on Windows

Important note:

- Spark 4.3.0 is used in the container image.
- Hadoop 3.4.2 is the HDFS runtime version used for the cluster.
- Java 17 is the required JDK for both Hadoop and Spark.
- The Streamlit dashboard reads the processed CSV files and does not need Hadoop or Spark libraries installed in the dashboard environment.

## Dashboard dependency configuration

The dashboard requirements in `requirements.txt` include the packages used by the UI:

- `streamlit`
- `pandas`
- `plotly`

Additional third-party packages are not added unless they are directly imported by the dashboard code. This keeps the dashboard environment lightweight and reproducible while leaving cluster-specific runtime dependencies to the Docker image.

## Docker Desktop on Windows + WSL2

Docker is not installed in the current environment. On a Windows laptop, use Docker Desktop for Windows with WSL2 integration.

### 1. Check WSL2 status

Open PowerShell as Administrator:

```powershell
wsl --status
wsl -l -v
```

### 2. Check the Ubuntu distribution

From the Ubuntu terminal:

```bash
cat /etc/os-release
```

### 3. Install Docker Desktop

Download the official installer from:

https://www.docker.com/products/docker-desktop/

Then install Docker Desktop for Windows and complete the setup wizard.

### 4. Enable WSL 2 based engine

In Docker Desktop:

- Settings > General
- Enable `Use the WSL 2 based engine`

### 5. Enable Ubuntu integration

In Docker Desktop:

- Settings > Resources > WSL Integration
- Enable the Ubuntu distribution used by your project

### 6. Restart Docker Desktop and WSL

If Docker is not responding:

- close Docker Desktop
- reopen it
- restart the Ubuntu WSL session if needed

### 7. Verify Docker from Ubuntu terminal

```bash
docker --version
docker compose version
docker run hello-world
```

If these commands work, Docker Desktop is ready for the cluster.

## Starting the three-node cluster

From the repo root:

```bash
docker compose up --build -d
```

Verify all services are running:

```bash
docker compose ps
```

Service endpoints:

- HDFS NameNode: http://localhost:9870
- Spark Master UI: http://localhost:8080
- HDFS RPC: hdfs://master:9000
- Spark Master: spark://master:7077

## Cluster startup behavior

The startup scripts are designed to:

- format the NameNode only the first time a fresh volume is created
- avoid reformatting the NameNode on every restart
- wait for the NameNode to become ready before starting the DataNodes
- start the HDFS DataNodes and Spark workers on `worker1` and `worker2`
- keep HDFS and Spark services running in the foreground so the container remains alive

## HDFS configuration summary

`config/core-site.xml`

```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://master:9000</value>
    </property>
</configuration>
```

`config/hdfs-site.xml`

```xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>2</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>/hadoop/dfs/name</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>/hadoop/dfs/data</value>
    </property>
</configuration>
```

## Spark configuration summary

The cluster is configured with:

- Spark Master URL: `spark://master:7077`
- Spark Master UI: `http://localhost:8080`
- two Spark workers:
  - `worker1`
  - `worker2`

## Running the analytics job

After the Docker cluster is up, the project can still use the existing Spark analytics script in `src/ratings_analysis.py` with the cluster endpoints.

From the repository root:

```bash
cd /home/jahnavi/MovieLensProject
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

If you want to submit the analytics job from the cluster environment, use the containerized Python runtime or any environment that has the project dependencies installed. The existing analytics script reads the raw data from HDFS and exports processed files to the local `data/processed` directory as the project already expects.

## Streamlit dashboard

Start the dashboard from the repository root:

```bash
streamlit run dashboard/app.py
```

The dashboard reads processed analytics files from `data/processed/` and displays:

- KPI cards for total ratings, movies, users, and average rating
- genre comparison charts
- rating distribution charts
- top movies filtered by minimum rating count
- top active users

## Data pipeline

The overall pipeline continues to work as follows:

1. The MovieLens raw files are loaded.
2. The PySpark analysis job computes ratings and movie statistics.
3. Results are exported as CSV files.
4. The Streamlit dashboard reads those CSV outputs.

This is the same business logic as the original project, with the infrastructure upgraded to a containerized Hadoop + Spark cluster.

## Useful commands

Check cluster health:

```bash
docker compose ps
docker logs master
docker logs worker1
docker logs worker2
```

Stop the cluster:

```bash
docker compose down
```

Optional: remove persistent data volumes if you want a fresh state:

```bash
docker volume rm movielensproject_namenode-data movielensproject_datanode1-data movielensproject_datanode2-data
```

Warning: removing Docker volumes destroys HDFS metadata and DataNode state. Only do this when you intentionally want to reset the cluster.

## Notes

- The Docker setup is designed to be reproducible and safe for local development.
- It keeps the original project code, data, and dashboard intact.
- It does not force a rebuild of the application from scratch.
- It does not delete persistent cluster data unless explicitly requested.

