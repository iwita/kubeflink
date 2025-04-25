```sh
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.11.0
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator
```


### Example: Submit a Flink Job
```sh
kubectl create -f https://raw.githubusercontent.com/apache/flink-kubernetes-operator/release-1.11/examples/basic.yaml
```

Check logs
```sh
kubectl logs -f deploy/basic-example

```

Expose the Flink Dashboard
```sh
kubectl port-forward svc/basic-example-rest 8081
```

Delete the deployment
```sh
kubectl delete flinkdeployment/basic-example
```