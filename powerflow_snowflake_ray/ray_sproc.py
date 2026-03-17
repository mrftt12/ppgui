from abc import abstractmethod
import os
from pathlib import Path
import shutil
from snowflake.snowpark import Session
from snowflake.snowpark.types import StringType
import snowflake.snowpark
from powerflow_snowflake_ray.util import stage, job_stage


MULTICONDUCTOR_VERSION='0.0.1.10'

print(f"SNOWFLAKE_ACCOUNT: {os.environ.get('SNOWFLAKE_ACCOUNT')}")
print(f"SNOWFLAKE_USER: {os.environ.get('SNOWFLAKE_USER')}")
print(f"SNOWFLAKE_ROLE: {os.environ.get('SNOWFLAKE_ROLE')}")
print(f"SNOWFLAKE_WAREHOUSE: {os.environ.get('SNOWFLAKE_WAREHOUSE')}")

connection_parameters = {
    "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
    "user": os.environ.get("SNOWFLAKE_USER"),
    'role': os.environ.get("SNOWFLAKE_ROLE"),
    "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
    "database": os.environ.get("SNOWFLAKE_DATABASE"),
    "schema": os.environ.get("SNOWFLAKE_SCHEMA"),
    "PYTHON_UDTF_END_PARTITION_TIMEOUT_SECONDS": 3600,
    "authenticator": "externalbrowser",
}


class RaySProcClient:
    def __init__(
        self,
        stage_name,
        pkg_name,
        pkg_src,
        recreate_stage=False,
        local_staging_dir="tmp",
    ):
        self.stage_name = stage_name
        self.pkg_name = pkg_name
        self.local_staging_dir = local_staging_dir
        self.pkg_src = pkg_src
        self.recreate_stage = recreate_stage
        for key, value in connection_parameters.items():
            if not value:
                raise ValueError(
                    f"Missing required environment variable: SNOWFLAKE_{key.upper()}"
                )

    def upload_pkg(self, session: snowflake.snowpark.Session, dependencies):
        if os.path.isdir(self.pkg_src):
            shutil.make_archive(
                f"{self.local_staging_dir}/{self.pkg_name}", "zip", self.pkg_src
            )
        session.file.put(
            f"{self.local_staging_dir}/{self.pkg_name}.zip",
            f"@{self.stage_name}",
            overwrite=True,
        )
        dependencies.append(f"@{self.stage_name}/{self.pkg_name}.zip")
        print(f"Uploaded 'powerflow.zip' to stage '{self.stage_name}'.")

        if self.recreate_stage:
            if os.path.isdir(f"{self.local_staging_dir}/pandapower-3.1.2"):
                shutil.make_archive(
                    f"{self.local_staging_dir}/pandapower",
                    "zip",
                    f"{self.local_staging_dir}/pandapower-3.1.2",
                )
            session.file.put(
                f"{self.local_staging_dir}/pandapower.zip",
                f"@{self.stage_name}",
                overwrite=True,
            )
            print(f"Uploaded 'pandapower.zip' to stage '{self.stage_name}'.")
        dependencies.append(f"@{self.stage_name}/pandapower.zip")

        if self.recreate_stage:
            if os.path.isdir(f"{self.local_staging_dir}/multiconductor-{MULTICONDUCTOR_VERSION}"):
                shutil.make_archive(
                    f"{self.local_staging_dir}/multiconductor",
                    "zip",
                    f"{self.local_staging_dir}/multiconductor-{MULTICONDUCTOR_VERSION}",
                )
            session.file.put(
                f"{self.local_staging_dir}/multiconductor.zip",
                f"@{self.stage_name}",
                overwrite=True,
            )
            print(f"Uploaded 'multiconductor.zip' to stage '{self.stage_name}'.")
        dependencies.append(f"@{self.stage_name}/multiconductor.zip")

        if self.recreate_stage:
            if os.path.isdir(f"{self.local_staging_dir}/dotted_dict-1.1.3"):
                shutil.make_archive(
                    f"{self.local_staging_dir}/dotted_dict",
                    "zip",
                    f"{self.local_staging_dir}/dotted_dict-1.1.3",
                )
            session.file.put(
                f"{self.local_staging_dir}/dotted_dict.zip",
                f"@{self.stage_name}",
                overwrite=True,
            )
            print(f"Uploaded 'dotted_dict.zip' to stage '{self.stage_name}'.")
        dependencies.append(f"@{self.stage_name}/dotted_dict.zip")

        if os.path.isfile(f"{self.local_staging_dir}/env.json"):
            session.file.put(
                f"{self.local_staging_dir}/env.json",
                f"@{self.stage_name}",
                overwrite=True,
            )
            print(f"Uploaded {self.local_staging_dir}/env.json to stage '{self.stage_name}'.")        

    def create_stage(self, session):
        if self.recreate_stage:
            session.sql(f"CREATE OR REPLACE STAGE {self.stage_name} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') DIRECTORY = (ENABLE = TRUE)").collect()
            session.sql(f"CREATE OR REPLACE STAGE {stage}").collect()
        else:
            session.sql(f"CREATE STAGE IF NOT EXISTS {self.stage_name} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') DIRECTORY = (ENABLE = TRUE)").collect()
            session.sql(f"CREATE STAGE IF NOT EXISTS {stage}").collect()
        print(f"Stage '{self.stage_name}' created or replaced.")

    def get_staged_packages(self, session):
        cur = session.connection.cursor()
        cur.execute(f"LIST @{self.stage_name}")
        package_list = []
        for row in cur:
            print(row)
            pkg = row[0].split("/")
            if len(pkg) == 2:
                package_list.append(pkg[1])
        return package_list

    def register_sproc(
        self, session: snowflake.snowpark.Session, imports, procedure_name, func
    ):
        session.sproc.register(
            func=func,
            execute_as='caller',
            name=procedure_name,
            return_type=StringType(),
            packages=[
                "snowflake-snowpark-python==1.42.0",
                "geojson",
                "deepdiff",
                "lxml",
                # "dotted_dict", #not available, added to stage
                "numba",
                "typing_extensions==4.15.0",
                "numpy==1.26.4",
                "packaging==25.0",
                "tqdm==4.67.1",
                "pandas==2.3.2",
                "networkx==3.4.2",
                "scipy==1.15.3",
                "Jinja2",
                "snowflake-ml-python",
            ],
            imports=imports,
            is_permanent=True,
            stage_location=f"@{self.stage_name}",
            replace=True,
        )

    def _call_sproc(self, session, procedure_name, *args):
        result = session.call(procedure_name, *args)
        print(f"Stored procedure result: {result}")
        return result

    def run(self, procedure_name, *args):
        with Session.builder.configs(connection_parameters).create() as session:
            return self._call_sproc(session, procedure_name, *args)
    
    def register_proc_and_run(self, procedure_name, func, *args):
        with Session.builder.configs(connection_parameters).create() as session:
            session.custom_package_usage_config["enabled"] = True
            print("Connected to Snowflake.")
            self.create_stage(session)
            dependencies = []
            self.upload_pkg(session, dependencies)
            self.register_sproc(session, dependencies, procedure_name, func)
            result = self._call_sproc(session, procedure_name, *args)

            with session.connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT * 
                    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
                """
                )

                results = cursor.fetchall()
                print("Procedure results:")
                for row in results:
                    print(row)
            return result

    def install(self, procs: dict):
        with Session.builder.configs(connection_parameters).create() as session:
            session.custom_package_usage_config["enabled"] = True
            print("Connected to Snowflake.")
            self.create_stage(session)
            dependencies = []
            self.upload_pkg(session, dependencies)
            for proc_name, handler in procs.items():
                self.register_sproc(session, dependencies, proc_name, handler)
