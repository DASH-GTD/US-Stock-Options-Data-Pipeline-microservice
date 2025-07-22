brew install azure-cli

az login

az account show

az group create --name stockPipelineResource --location northeurope

az aks create \
  --resource-group stockPipelineResource \
  --name stockCluster \
  --node-count 2 \
  --node-vm-size Standard_B2s \
  --enable-addons monitoring \
  --generate-ssh-keys \
  --load-balancer-sku standard

az aks get-credentials --resource-group stockPipelineResource --name stockCluster 

kubectl get nodes

kubectl create secret docker-registry dockerhub-secret \
  --docker-server=https://index.docker.io/v1/ \
  --docker-username=<docker-username> \
  --docker-password=<docker-tocken> \
  --docker-email=harshdeepmahajan88@gmail.com

chmod +x script/helm.sh 
./script/helm.sh 

chmod +x script/build_image.sh
./script/build_image.sh 

kubectl apply -f manifests