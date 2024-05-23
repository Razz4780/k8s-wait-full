# Usage

Basic usage: `k8s-wait-full Deployment my-deployment -f - < deployment-filter.yaml`

Run `k8s-wait-full --help` for more options.

# State filter

Example state filter:

```yaml
status:
  availableReplicas: 4
spec:
  replicas: 4
  selector:
    matchLabels:
      my-label: label-value
  template:
    spec:
      containers:
        - name: my-container
          image: my-image
```

When applied to a deployment, it will match when all following conditions are met:
1. Deployment has 4 available replicas,
2. Deployment is configured to have 4 replicas,
3. Deployment is has label `my-label` with value `label-value`,
4. Deployment has a container named `my-container` that runs image `my-image`.
