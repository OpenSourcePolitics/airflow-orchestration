# 🚀 Airflow Deployment Guide

This repository manages the deployment of **Apache Airflow** using Docker images and DAGs, versioned through Git tags. The deployment workflow is automated with **GitHub Actions** for building and pushing images and **FluxCD** for synchronizing with Kubernetes.

## 🛠️ Prerequisites

Before starting a new deployment, ensure that:

- You are on the `main` branch and up to date with the latest changes:
  ```bash
  git checkout main
  git pull origin main
  ```
  
---

## 📌 Deployment Workflow

The deployment process involves two key versioned components:

1. **Airflow Docker Image (`airflow-vX.X.X`)**  
   - Contains Apache Airflow and required dependencies.
   - Needs to be updated when new dependencies or fixes are required.

```bash
git tag -a airflow-vX.X.X -m "Publishing version X.X.X of the Airflow image"
git push origin airflow-vX.X.X
```

2. **DAGs Version (`dags-vX.X.X`)**  
   - Contains Airflow DAGs.
   - Needs to be updated when DAGs are modified or new DAGs are added.

```bash
git tag -a dags-vX.X.X -m "Publishing version X.X.X of the Dags"
git push origin dags-vX.X.X
```
   - Then, in `prod/flux-sync.yml`, edit the `spec.values.dags.gitSync.rev` value to use the proper version of the dags or airflow image
```
        rev: "refs/tags/dags-vX.X.X"

```
   - Use a commit message like : [Prod] - Release dags-vX.X.X