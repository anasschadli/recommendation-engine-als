# Apache Airflow Setup Guide

## Quick Start (Docker)

### 1. Start Services

```bash
docker-compose up -d
```

Wait for all containers to be healthy:

```bash
docker-compose ps
```

### 2. Access Airflow UI

- URL: http://localhost:8081
- Username: `admin`
- Password: `admin`

---

## Using the DAG

### View the Pipeline

1. Go to http://localhost:8081
2. Find `recommendation_pipeline` in the DAG list
3. View the pipeline structure in "Graph" tab

### Trigger Manually

**Via Web UI:**

- Click "Trigger DAG" button

**Via CLI:**

```bash
docker-compose exec airflow-webserver airflow dags trigger recommendation_pipeline
```

### Monitor Execution

1. Click on a task in Graph View
2. Go to "Log" tab to see output
3. Check status: Tree View shows run history

---

## Useful Commands

```bash
# View all DAGs
docker-compose exec airflow-webserver airflow dags list

# List users
docker-compose exec airflow-webserver airflow users list

# View logs
docker-compose logs airflow-webserver

# Stop all services
docker-compose down

# Restart services
docker-compose restart
```