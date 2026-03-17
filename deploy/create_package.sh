SRC_STAGE_FOLDER=../../tmp
TARGET_FOLDER=/root/work-WSL2/GridPlanning/powerflow_r8
TARGET_STAGE_FOLDER=$TARGET_FOLDER/_staging
mkdir -p $TARGET_STAGE_FOLDER
mkdir -p $TARGET_FOLDER/deploy
mkdir -p $TARGET_FOLDER/powerflow_pipeline
mkdir -p $TARGET_FOLDER/powerflow_snowflake
# 
cp -R ../../.devcontainer $TARGET_FOLDER
cp Dockerfile $TARGET_FOLDER
# 
cp snowflake_procedure_runner.py $TARGET_FOLDER/deploy
cp ../../src/powerflow_pipeline/util.py $TARGET_FOLDER/powerflow_pipeline
cp procedure.py $TARGET_FOLDER/powerflow_snowflake
# 
cp run_powerflow.py $TARGET_FOLDER
cp define_circuits.py $TARGET_FOLDER
cp install.py $TARGET_FOLDER
cp ingest_circuit_data.py $TARGET_FOLDER
cp ingest_profile_data.py $TARGET_FOLDER
# 
cp env.json $TARGET_FOLDER
cp env.json $SRC_STAGE_FOLDER
cp requirements.txt $TARGET_FOLDER
cp INSTALLATION.md $TARGET_FOLDER

# cat > temp_script.py << 'EOF'
# import os
# import shutil
# local_staging_dir='tmp'
# pkg_name = 'powerflow'

# pkg_src = 'src'
# print(f"Creating zip -> {local_staging_dir}/{pkg_name}")
# if os.path.isdir(pkg_src):
#     shutil.make_archive(
#         f"{local_staging_dir}/{pkg_name}", "zip", pkg_src
#     )
# EOF
# python temp_script.py "$@"
# 
cp $SRC_STAGE_FOLDER/powerflow.zip $TARGET_STAGE_FOLDER
cp $SRC_STAGE_FOLDER/pandapower.zip $TARGET_STAGE_FOLDER
cp $SRC_STAGE_FOLDER/multiconductor.zip $TARGET_STAGE_FOLDER
cp $SRC_STAGE_FOLDER/dotted_dict.zip $TARGET_STAGE_FOLDER
cp $SRC_STAGE_FOLDER/dotted_dict.zip $TARGET_STAGE_FOLDER
cp $SRC_STAGE_FOLDER/env.json $TARGET_STAGE_FOLDER

cat << EOF > $TARGET_FOLDER/.env
SNOWFLAKE_WAREHOUSE=<warehouse name>
SNOWFLAKE_PASSWORD=<password>
SNOWFLAKE_ACCOUNT=<account>
SNOWFLAKE_DATABASE=<database>
SNOWFLAKE_SCHEMA=<schema>
SNOWFLAKE_USER=<user name>
SNOWFLAKE_AUTHENTICATOR=<authenticator>
EOF
