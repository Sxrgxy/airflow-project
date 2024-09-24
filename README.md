# docker-airflow-spark

Docker with Airflow + Postgres + Spark cluster

## The Containers

- **airflow-webserver**: Airflow webserver and scheduler, with spark-submit support.
  - image: `docker-airflow2:latest` (custom, Airflow version 2.2.4)
    - Based on python:3.7-stretch, [puckel/docker-airflow](https://github.com/puckel/docker-airflow) and [cordon-thiago/airflow-spark](https://github.com/cordon-thiago/airflow-spark)
  - port: `8080`

- **postgres**: Postgres database, used by Airflow.
  - image: `postgres:13.6`
  - port: `5432`

- **spark-master**: Spark Master.
  - image: `bitnami/spark:3.2.1`
  - port: `8081`

- **spark-worker[-N]**: Spark workers (default number: 1). Modify `docker-compose.yml` file to add more.
  - image: `bitnami/spark:3.2.1`

## 🔧 Setup

### Clone project

```bash
git clone https://github.com/Sxrgxy/airflow-project
```

### Build airflow Docker

```bash
cd airflow-project/docker/
docker-compose up -d --build
```

### Check accesses

- **Airflow**: [http://localhost:8080](http://localhost:8080)
- **Spark Master**: [http://localhost:8081](http://localhost:8081)

## 👣 Additional steps

### Edit connection from Airflow to Spark

- Go to Airflow UI > Admin > Edit connections
- Edit `spark_default` entry:
  - Connection Type: Spark
  - Host: spark://spark
  - Port: 7077

### Test spark-submit from Airflow

Go to the Airflow UI and run the `daily_script_run` DAG :)
```