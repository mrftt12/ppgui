@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM Cloud Run Deployment Script for LoadFlow UI (Windows BAT, no subroutines)

REM Default configuration (can be overridden by environment variables)
if not defined GCP_PROJECT_ID set "GCP_PROJECT_ID=pspsopsgit-22535999-367b6"
if not defined GCP_REGION set "GCP_REGION=us-central1"
if not defined BACKEND_SERVICE_NAME set "BACKEND_SERVICE_NAME=loadflow-backend"
if not defined FRONTEND_SERVICE_NAME set "FRONTEND_SERVICE_NAME=loadflow-frontend"
if not defined ENVIRONMENT set "ENVIRONMENT=production"
if not defined TAG_KEY_NAME set "TAG_KEY_NAME=environment"
if not defined TAG_VALUE_NAME set "TAG_VALUE_NAME=Production"

set "PROJECT_ID=%GCP_PROJECT_ID%"
set "REGION=%GCP_REGION%"
set "BACKEND_IMAGE_NAME=gcr.io/%PROJECT_ID%/%BACKEND_SERVICE_NAME%"
set "FRONTEND_IMAGE_NAME=gcr.io/%PROJECT_ID%/%FRONTEND_SERVICE_NAME%"

echo Starting Cloud Run deployment...

echo Checking prerequisites...
where gcloud >nul 2>nul
if errorlevel 1 (
  echo Error: gcloud CLI is not installed. Please install it from https://cloud.google.com/sdk/docs/install
  exit /b 1
)
where docker >nul 2>nul
if errorlevel 1 (
  echo Error: Docker is not installed. Please install it from https://docs.docker.com/get-docker/
  exit /b 1
)
echo Prerequisites check passed

echo Validating configuration...
if "%PROJECT_ID%"=="" (
  echo Error: GCP_PROJECT_ID is not set. Please set it as an environment variable or in this script.
  echo Example: set GCP_PROJECT_ID=your-project-id
  exit /b 1
)
echo Configuration validated
echo   Project ID: %PROJECT_ID%
echo   Region: %REGION%
echo   Backend Service: %BACKEND_SERVICE_NAME%
echo   Frontend Service: %FRONTEND_SERVICE_NAME%

echo Ensuring environment tag (Production)...
set "PARENT_TYPE="
set "PARENT_ID="
for /f "delims=" %%i in ('gcloud projects describe "!PROJECT_ID!" --format="value(parent.type)"') do set "PARENT_TYPE=%%i"
for /f "delims=" %%i in ('gcloud projects describe "!PROJECT_ID!" --format="value(parent.id)"') do set "PARENT_ID=%%i"
set "TAG_PARENT="
if /I "!PARENT_TYPE!"=="organization" set "TAG_PARENT=organizations/!PARENT_ID!"
if /I "!PARENT_TYPE!"=="folder" set "TAG_PARENT=folders/!PARENT_ID!"
if "!TAG_PARENT!"=="" goto :tag_parent_missing
set "TAG_KEY="
for /f "delims=" %%i in ('gcloud resource-manager tags keys list --parent="!TAG_PARENT!" --filter="shortName=!TAG_KEY_NAME!" --format="value(name)"') do set "TAG_KEY=%%i"
if "!TAG_KEY!"=="" goto :create_tag_key
goto :tag_key_ready
:create_tag_key
gcloud resource-manager tags keys create !TAG_KEY_NAME! --parent="!TAG_PARENT!" --short-name=!TAG_KEY_NAME!
if errorlevel 1 exit /b 1
for /f "delims=" %%i in ('gcloud resource-manager tags keys list --parent="!TAG_PARENT!" --filter="shortName=!TAG_KEY_NAME!" --format="value(name)"') do set "TAG_KEY=%%i"
:tag_key_ready
if "!TAG_KEY!"=="" goto :tag_key_missing
set "TAG_VALUE="
for /f "delims=" %%i in ('gcloud resource-manager tags values list --parent="!TAG_KEY!" --filter="shortName=!TAG_VALUE_NAME!" --format="value(name)"') do set "TAG_VALUE=%%i"
if "!TAG_VALUE!"=="" goto :create_tag_value
goto :tag_value_ready
:create_tag_value
gcloud resource-manager tags values create !TAG_VALUE_NAME! --parent="!TAG_KEY!" --short-name=!TAG_VALUE_NAME!
if errorlevel 1 exit /b 1
for /f "delims=" %%i in ('gcloud resource-manager tags values list --parent="!TAG_KEY!" --filter="shortName=!TAG_VALUE_NAME!" --format="value(name)"') do set "TAG_VALUE=%%i"
:tag_value_ready
if "!TAG_VALUE!"=="" goto :tag_value_missing
set "PROJECT_NUMBER="
for /f "delims=" %%i in ('gcloud projects describe "!PROJECT_ID!" --format="value(projectNumber)"') do set "PROJECT_NUMBER=%%i"
if "!PROJECT_NUMBER!"=="" goto :project_number_missing
set "TAG_BINDING="
for /f "delims=" %%i in ('gcloud resource-manager tags bindings list --parent="projects/!PROJECT_NUMBER!" --location=global --filter="tagValue=!TAG_VALUE!" --format="value(name)"') do set "TAG_BINDING=%%i"
if "!TAG_BINDING!"=="" goto :create_tag_binding
echo Environment tag already set.
goto :after_tagging
:create_tag_binding
gcloud resource-manager tags bindings create --tag-value="!TAG_VALUE!" --parent="projects/!PROJECT_NUMBER!" --location=global
if errorlevel 1 exit /b 1
goto :after_tagging
:tag_parent_missing
echo Error: Project has no organization or folder parent. Tag keys must be created under an organization or folder.
echo Please set TAG_PARENT (organizations/ORG_ID or folders/FOLDER_ID) and rerun.
exit /b 1
:tag_key_missing
echo Error: Unable to resolve environment tag key.
exit /b 1
:tag_value_missing
echo Error: Unable to resolve Production tag value.
exit /b 1
:project_number_missing
echo Error: Unable to resolve project number.
exit /b 1
:after_tagging
echo Configuring gcloud...
gcloud config set project "%PROJECT_ID%"
if errorlevel 1 exit /b 1
echo Enabling required Google Cloud APIs...
gcloud services enable ^
  cloudbuild.googleapis.com ^
  run.googleapis.com ^
  containerregistry.googleapis.com ^
  --project="%PROJECT_ID%"
if errorlevel 1 exit /b 1
echo gcloud configured

echo Building backend Docker image...
docker build --platform linux/amd64 -t "%BACKEND_IMAGE_NAME%" -f backend/Dockerfile .
if errorlevel 1 exit /b 1
echo Pushing backend image to GCR...
docker push "%BACKEND_IMAGE_NAME%"
if errorlevel 1 exit /b 1
echo Backend image built and pushed

echo Deploying backend to Cloud Run...
gcloud run deploy "%BACKEND_SERVICE_NAME%" ^
  --image "%BACKEND_IMAGE_NAME%" ^
  --platform managed ^
  --region "%REGION%" ^
  --allow-unauthenticated ^
  --memory 2Gi ^
  --cpu 2 ^
  --timeout 300 ^
  --max-instances 10 ^
  --min-instances 0 ^
  --set-env-vars "PYTHONUNBUFFERED=1" ^
  --project "%PROJECT_ID%"
if errorlevel 1 exit /b 1
for /f "delims=" %%i in ('gcloud run services describe "%BACKEND_SERVICE_NAME%" --platform managed --region "%REGION%" --format "value(status.url)" --project "%PROJECT_ID%"') do set "BACKEND_URL=%%i"
echo Backend deployed successfully
echo   Backend URL: %BACKEND_URL%

echo Building frontend Docker image...
pushd frontend
docker build --platform linux/amd64 --build-arg REACT_APP_BACKEND_URL="%BACKEND_URL%" -t "%FRONTEND_IMAGE_NAME%" .
if errorlevel 1 (
  popd
  exit /b 1
)
echo Pushing frontend image to GCR...
docker push "%FRONTEND_IMAGE_NAME%"
if errorlevel 1 (
  popd
  exit /b 1
)
popd
echo Frontend image built and pushed

echo Deploying frontend to Cloud Run...
gcloud run deploy "%FRONTEND_SERVICE_NAME%" ^
  --image "%FRONTEND_IMAGE_NAME%" ^
  --platform managed ^
  --region "%REGION%" ^
  --allow-unauthenticated ^
  --memory 512Mi ^
  --cpu 1 ^
  --timeout 60 ^
  --max-instances 10 ^
  --min-instances 0 ^
  --set-env-vars "REACT_APP_BACKEND_URL=%BACKEND_URL%" ^
  --project "%PROJECT_ID%"
if errorlevel 1 exit /b 1
for /f "delims=" %%i in ('gcloud run services describe "%FRONTEND_SERVICE_NAME%" --platform managed --region "%REGION%" --format "value(status.url)" --project "%PROJECT_ID%"') do set "FRONTEND_URL=%%i"
echo Frontend deployed successfully
echo   Frontend URL: %FRONTEND_URL%

echo =========================================
echo Deployment completed successfully!
echo =========================================
echo Backend Service: %BACKEND_URL%
echo Frontend Service: %FRONTEND_URL%
echo.
echo To view logs:
echo   Backend:  gcloud run logs tail %BACKEND_SERVICE_NAME% --region %REGION%
echo   Frontend: gcloud run logs tail %FRONTEND_SERVICE_NAME% --region %REGION%
echo.
echo To update services, run this script again.

exit /b 0
