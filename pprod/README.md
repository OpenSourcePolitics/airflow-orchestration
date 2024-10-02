# k8s

## PG

```
k apply -f ./pg.yml
```

Get secret:
```
PASS=`k get secret airflow.airflow--pg.credentials.postgresql.acid.zalan.do -o json | jq  -r '.data.password' | base64 -d`
```

Get service:
```
SVC=`k get svc -l spilo-role=master --no-headers -o custom-columns=NAME:.metadata.name`
```

Create connection string secret:
```
kubectl create secret generic airflow-connection-string  --from-literal=connection=postgresql://airflow:${PASS}@${SVC}:5432/airflow
```

## Flux

```
k apply -f ./flux-sync.yml
```
