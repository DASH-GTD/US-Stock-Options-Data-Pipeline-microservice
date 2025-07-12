# docker compose build 
# kind load docker-image backend-airflow:latest --name kind
# kind load docker-image backend-data-collector:latest --name kind
# kind load docker-image backend-data-processor:latest --name kind
# kind load docker-image backend-database-writer:latest --name kind
# kind load docker-image backend-file-writer:latest --name kind
# kind load docker-image backend-data-api-service:latest --name kind



#!/bin/bash

# Set your Docker Hub username (or other registry)
REGISTRY_USER="hdm08"
TAG="latest"

# Build all images using Docker Compose
docker compose build

# List of service image names
services=(
  backend-airflow
  backend-data-collector
  backend-data-processor
  backend-database-writer
  backend-file-writer
  backend-data-api-service
)

# Tag and push each image
for service in "${services[@]}"
do
  IMAGE_NAME="$REGISTRY_USER/$service:$TAG"
  
  echo "Tagging image: $service -> $IMAGE_NAME"
  docker tag $service $IMAGE_NAME

  echo "Pushing image: $IMAGE_NAME"
  docker push $IMAGE_NAME
done

echo "✅ All images built and pushed successfully!"
