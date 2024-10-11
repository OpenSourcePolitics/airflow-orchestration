# k8s

## PG

```
k apply -f ./pg.yml
```

Get secret:
```
PASS=`k -n data-prod get secret airflow.airflow--pg.credentials.postgresql.acid.zalan.do -o json | jq  -r '.data.password' | base64 -d`
```

Get service:
```
SVC=`k -n data-prod get svc -l spilo-role=master --no-headers -o custom-columns=NAME:.metadata.name`
```

Create connection string secret:
```
kubectl -n data-prod create secret generic airflow-connection-string  --from-literal=connection=postgresql://airflow:${PASS}@${SVC}:5432/airflow
```

## Flux

```
k -n data-prod apply -f ./flux-sync.yml
```

## Web

Change admin password in WebUI.

## Fernet key

```
echo Fernet Key: $(kubectl get secret --namespace data-prod airflow-fernet-key -o jsonpath="{.data.fernet-key}" | base64 --decode)
```

## Debug

Check image tag:
```
k edit po airflow-worker-0
```

## force sync

```
flux -n data-prod reconcile source git airflow
```

## Code de la chart helm

https://github.com/airflow-helm/charts/tree/main/charts/airflow
