# k8s

## PG

```
k apply -f ./pg.yml
```

Get secret:
```
PASS=`k -n data-pprod get secret airflow.airflow--pg.credentials.postgresql.acid.zalan.do -o json | jq  -r '.data.password' | base64 -d`
```

Get service:
```
SVC=`k -n data-pprod get svc -l spilo-role=master --no-headers -o custom-columns=NAME:.metadata.name`
```

Create connection string secret:
```
kubectl -n data-pprod create secret generic airflow-connection-string  --from-literal=connection=postgresql://airflow:${PASS}@${SVC}:5432/airflow
```

## Flux

```
k -n data-pprod apply -f ./flux-sync.yml
```

## Web

Change admin password in WebUI.

## Fernet key

```
echo Fernet Key: $(kubectl get secret --namespace data-pprod airflow-fernet-key -o jsonpath="{.data.fernet-key}" | base64 --decode)
```

## Debug

Check image tag:
```
k edit po airflow-worker-0
```
