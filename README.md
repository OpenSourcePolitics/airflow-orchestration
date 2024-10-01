# airflow-dags

This repo manages DAGs ran by airflow.

## Coding standards

There's a pre-commit hook:
- a linter, `black`, may change your code ~> you may need to redo your `git add`/`git commit` after it passed
- flake8 will prevent you from committing code with problems

Please note that tests are not run by the pre-commit hook. It's your responsibility to do it.

flake8 and tests are ran on the CI.

Activate pre-commit in local:
```
# install pre-commit package
pip install pre-commit

# launch pre-commit for every git commit
pre-commit install
```


## About requirements

Goes into requirements.txt only what goes to production.
- Do not include development dependencies (precommit, black...)

## About Docker compose
You need to add in your profile

```export AIRFLOW_IMAGE_NAME=airflow_image_2.10.0```

Then build the image

```docker build -t $AIRFLOW_IMAGE_NAME .```

Once this done you can initialize Airflow env

```docker-compose up airflow-init```

And if it succeed, to start the instance

```docker-compose up -d```

And to stop the instance

```docker-compose down```
