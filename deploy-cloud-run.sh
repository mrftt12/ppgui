#!/bin/bash

# Cloud Run Deployment Script for LoadFlow UI
# This script deploys both frontend and backend services to Google Cloud Run

GCP_PROJECT_ID='pspsopsgit-22535999-367b6'

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration variables
PROJECT_ID="${GCP_PROJECT_ID:-}"
REGION="${GCP_REGION:-us-central1}"
BACKEND_SERVICE_NAME="${BACKEND_SERVICE_NAME:-loadflow-backend}"
FRONTEND_SERVICE_NAME="${FRONTEND_SERVICE_NAME:-loadflow-frontend}"
BACKEND_IMAGE_NAME="gcr.io/${PROJECT_ID}/${BACKEND_SERVICE_NAME}"
FRONTEND_IMAGE_NAME="gcr.io/${PROJECT_ID}/${FRONTEND_SERVICE_NAME}"

# Function to print colored messages
print_message() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Function to check if required tools are installed
check_prerequisites() {
    print_message "$YELLOW" "Checking prerequisites..."
    
    if ! command -v gcloud &> /dev/null; then
        print_message "$RED" "Error: gcloud CLI is not installed. Please install it from https://cloud.google.com/sdk/docs/install"
        exit 1
    fi
    
    if ! command -v docker &> /dev/null; then
        print_message "$RED" "Error: Docker is not installed. Please install it from https://docs.docker.com/get-docker/"
        exit 1
    fi
    
    print_message "$GREEN" "✓ Prerequisites check passed"
}

# Function to validate configuration
validate_config() {
    print_message "$YELLOW" "Validating configuration..."
    
    if [ -z "$PROJECT_ID" ]; then
        print_message "$RED" "Error: GCP_PROJECT_ID is not set. Please set it as an environment variable or in this script."
        echo "Example: export GCP_PROJECT_ID=your-project-id"
        exit 1
    fi
    
    print_message "$GREEN" "✓ Configuration validated"
    echo "  Project ID: $PROJECT_ID"
    echo "  Region: $REGION"
    echo "  Backend Service: $BACKEND_SERVICE_NAME"
    echo "  Frontend Service: $FRONTEND_SERVICE_NAME"
}

# Function to configure gcloud
configure_gcloud() {
    print_message "$YELLOW" "Configuring gcloud..."
    
    gcloud config set project "$PROJECT_ID"
    
    # Enable required APIs
    print_message "$YELLOW" "Enabling required Google Cloud APIs..."
    gcloud services enable \
        cloudbuild.googleapis.com \
        run.googleapis.com \
        containerregistry.googleapis.com \
        --project="$PROJECT_ID"
    
    print_message "$GREEN" "✓ gcloud configured"
}

# Function to build and push backend image
build_backend() {
    print_message "$YELLOW" "Building backend Docker image..."
    # Build the image for linux/amd64 platform (required for Cloud Run)
    docker build --platform linux/amd64 -t "$BACKEND_IMAGE_NAME" -f backend/Dockerfile .
    
    # Push to Google Container Registry
    print_message "$YELLOW" "Pushing backend image to GCR..."
    docker push "$BACKEND_IMAGE_NAME"
    
    print_message "$GREEN" "✓ Backend image built and pushed"
}

# Function to deploy backend to Cloud Run
deploy_backend() {
    print_message "$YELLOW" "Deploying backend to Cloud Run..."
    
    gcloud run deploy "$BACKEND_SERVICE_NAME" \
        --image "$BACKEND_IMAGE_NAME" \
        --platform managed \
        --region "$REGION" \
        --allow-unauthenticated \
        --memory 2Gi \
        --cpu 2 \
        --timeout 300 \
        --max-instances 10 \
        --min-instances 0 \
        --set-env-vars "PYTHONUNBUFFERED=1" \
        --project "$PROJECT_ID"
    
    # Get backend URL
    BACKEND_URL=$(gcloud run services describe "$BACKEND_SERVICE_NAME" \
        --platform managed \
        --region "$REGION" \
        --format 'value(status.url)' \
        --project "$PROJECT_ID")
    
    print_message "$GREEN" "✓ Backend deployed successfully"
    echo "  Backend URL: $BACKEND_URL"
}

# Function to build and push frontend image
build_frontend() {
    print_message "$YELLOW" "Building frontend Docker image..."
    
    cd frontend
    
    # Build the image with backend URL for linux/amd64 platform (required for Cloud Run)
    docker build \
        --platform linux/amd64 \
        --build-arg REACT_APP_BACKEND_URL="$BACKEND_URL" \
        -t "$FRONTEND_IMAGE_NAME" \
        .
    
    # Push to Google Container Registry
    print_message "$YELLOW" "Pushing frontend image to GCR..."
    docker push "$FRONTEND_IMAGE_NAME"
    
    cd ..
    
    print_message "$GREEN" "✓ Frontend image built and pushed"
}

# Function to deploy frontend to Cloud Run
deploy_frontend() {
    print_message "$YELLOW" "Deploying frontend to Cloud Run..."
    
    gcloud run deploy "$FRONTEND_SERVICE_NAME" \
        --image "$FRONTEND_IMAGE_NAME" \
        --platform managed \
        --region "$REGION" \
        --allow-unauthenticated \
        --memory 512Mi \
        --cpu 1 \
        --timeout 60 \
        --max-instances 10 \
        --min-instances 0 \
        --set-env-vars "REACT_APP_BACKEND_URL=$BACKEND_URL" \
        --project "$PROJECT_ID"
    
    # Get frontend URL
    FRONTEND_URL=$(gcloud run services describe "$FRONTEND_SERVICE_NAME" \
        --platform managed \
        --region "$REGION" \
        --format 'value(status.url)' \
        --project "$PROJECT_ID")
    
    print_message "$GREEN" "✓ Frontend deployed successfully"
    echo "  Frontend URL: $FRONTEND_URL"
}

# Function to display deployment summary
display_summary() {
    print_message "$GREEN" "\n========================================="
    print_message "$GREEN" "Deployment completed successfully!"
    print_message "$GREEN" "=========================================\n"
    echo "Backend Service: $BACKEND_URL"
    echo "Frontend Service: $FRONTEND_URL"
    echo ""
    echo "To view logs:"
    echo "  Backend:  gcloud run logs tail $BACKEND_SERVICE_NAME --region $REGION"
    echo "  Frontend: gcloud run logs tail $FRONTEND_SERVICE_NAME --region $REGION"
    echo ""
    echo "To update services, run this script again."
}

# Main deployment flow
main() {
    print_message "$GREEN" "Starting Cloud Run deployment...\n"
    
    check_prerequisites
    validate_config
    configure_gcloud
    build_backend
    deploy_backend
    build_frontend
    deploy_frontend
    display_summary
}

# Run main function
main
