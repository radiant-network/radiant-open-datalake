
## Start minikube
```
minikube start
```

Note: was tested with the following config:
```
minikube config set memory 12000
minikube config set cpus 4
```

## run minikube tunnel in a separated terminal
```
minikube tunnel
```

## Create a namespace for the project

```
kubectl create namespace opendatalake
```

## Switch to the open datalake namespace
```
kubectl config set-context --current --namespace=opendatalake
```

## Install MinIO
```
kubectl apply -f sandbox/k8s/minio/
```

## Monitor Minio pods are running (1 minutes)
```
kubectl get po | grep minio
```
Results 1 pod running and 1 pod completed:
```
opendatalake-minio-58c696c58c-2hkqz        1/1     Running     0          85s
opendatalake-minio-bucket-init-job-5hp2b   0/1     Completed   0          85s
```

## Install postgres
```
kubectl apply -f sandbox/k8s/postgres/
```

## Monitor Postgres pods are running (1 minutes)
```
kubectl get po | grep postgres
```
Results 1 pod running and 1 pod completed:
```
postgres-585445b9cc-gxnvx             1/1     Running     0          23s
postgres-init-job-fhtgf               0/1     Completed   0          23s
```

## Mount volume for dags in minikube

In a new terminal, run the following command to mount the dags directory in minikube:
```
minikube mount $(pwd)/dags:/opt/airflow/dags/dags
```

## Install airflow volumes for logs and dags
```
kubectl apply -f sandbox/k8s/airflow/
```

## Install Airflow

```
helm repo add apache-airflow https://airflow.apache.org
helm repo update
helm upgrade --install airflow apache-airflow/airflow -f sandbox/values/airflow-values.yaml
```
Took 5 minutes to install Airflow

## Monitor Airflow pod are running (5 minutes)
```
kubectl get po | grep airflow
```
Results 6 pods running:
```
airflow-api-server-6466bc98bd-gq7bj      1/1     Running     0          2m11s
airflow-dag-processor-58fcdfdb65-4rdkf   2/2     Running     0          2m11s
airflow-redis-0                          1/1     Running     0          2m11s
airflow-scheduler-f5df7bc8c-rpz25        2/2     Running     0          2m11s
airflow-statsd-688b56dc48-flndk          1/1     Running     0          2m11s
airflow-triggerer-0                      2/2     Running     0          2m11s
airflow-worker-0                         2/2     Running     0          2m11s
```

## Connect to the Airflow UI
Connect to the Airflow UI at http://localhost:8080
- Username: airflow
- Password: airflow