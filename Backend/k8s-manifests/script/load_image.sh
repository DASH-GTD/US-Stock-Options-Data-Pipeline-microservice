# Set your Docker Hub username (or other registry)
REGISTRY_USER="hdm08"
TAG="latest"

# List of service image names
services=(
  airflow_scheduler_service
  data_api_service
  data_collector_service
  data_processor_service
  database_writer_service
  file_writer_service
)

# Tag and push each image
for service in "${services[@]}"
do

  IMAGE_NAME="$REGISTRY_USER/$service:$TAG"

  echo "Pushing image: $IMAGE_NAME"
  docker push $IMAGE_NAME
done

echo "✅ All images built and pushed successfully!"
