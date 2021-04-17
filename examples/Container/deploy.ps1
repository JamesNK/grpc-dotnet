docker-compose -f .\docker-compose.yml build container-client
docker-compose -f .\docker-compose.yml build container-server

kubectl delete -f .\Kubernetes\deploy-server.yml
kubectl apply -f .\Kubernetes\deploy-server.yml

kubectl delete -f .\Kubernetes\deploy-client.yml
kubectl apply -f .\Kubernetes\deploy-client.yml