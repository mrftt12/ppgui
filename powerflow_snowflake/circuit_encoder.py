import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, row_number, uniform, lit, rank, row_number, random, mode
from snowflake.snowpark import Session

from powerflow_pipeline.util import PowerflowConfig
from snowflake.snowpark.functions import pandas_udtf, PandasDataFrameType
from snowflake.snowpark.types import (
    StringType,
    DecimalType,
    TimestampType,
    LongType,
    PandasDataFrame,
    StructField,
    DateType,
    IntegerType,
    BinaryType,
    ArrayType,
    StructType,
    FloatType,
    TimestampTimeZone,
    DoubleType,
)
import logging

from sce.sce_mc_mapping import SCECircuitDataTransformer, SCELoadProfileController, SCEPowerflowPipelineTemp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

OUTPUT_FIELDS = [
    StructField("POWER_SYSTEM_RESOURCE_MRID", StringType(), nullable=True),
]

LOAD_COLS = [
    "CIRCUIT_KEY",
    "FEEDER_MRID",
    "GROUP_ID",
]


class CircuitEncoder():
    def __init__(self, session: Session, pf_config: PowerflowConfig, circuit_keys: str, capacitor_enabled: bool,
                 generator_enabled: bool,
                 transformer_enabled: bool,
                 regulator_enabled: bool):
        self.session = session
        self.circuit_key_list = [c.strip() for c in circuit_keys.split(',')]
        self.capacitor_enabled = capacitor_enabled
        self.generator_enabled = generator_enabled
        self.transformer_enabled = transformer_enabled
        self.regulator_enabled = regulator_enabled
        self.pf_config = pf_config

    def encode_circuits(self):
        window_spec = snowpark.Window.partition_by(
            "CIRCUIT_KEY").order_by("CIRCUIT_KEY")
        circuits_df = (self.session.table(f"{self.pf_config.get_bus_table_database}.{self.pf_config.get_bus_table_schema()}.{self.pf_config.get_bus_table}")
                       .filter(col("CIRCUIT_KEY").in_(self.circuit_key_list))
                       .with_column("rn", row_number().over(window_spec))
                       .filter(col("rn") == 1)
                       .select("CIRCUIT_KEY", "FEEDER_MRID")
                       )

        window_spec = snowpark.Window.order_by(col("CIRCUIT_KEY"))

        df_with_sequence = circuits_df.with_column(
            "SEQUENCE_ID",
            row_number().over(window_spec)
        )

        df_final = df_with_sequence.with_column(
            "GROUP_ID",
            col("SEQUENCE_ID") % 5
        ).drop(col("SEQUENCE_ID"))

        stage_location = f"{self.pf_config.get_database()}.{self.pf_config.get_database_schema()}.{self.pf_config.get_stage_name()}"

        @pandas_udtf(
            packages=(
                "snowflake-snowpark-python",
                "geojson",
                "deepdiff",
                "lxml",
                "numba",
                "typing_extensions==4.15.0",
                "numpy==1.26.4",
                "packaging==25.0",
                "tqdm==4.67.1",
                "pandas==2.3.2",
                "networkx==3.4.2",
                "scipy==1.15.3",
                "Jinja2",
            ),
            name='CircuitEncoder',
            replace=True,
            stage_location=stage_location,
            output_schema=PandasDataFrameType(
                [f.datatype for f in OUTPUT_FIELDS], [
                    f.name for f in OUTPUT_FIELDS]
            ),
            input_types=[
                PandasDataFrameType(
                    [
                        StringType(),
                        StringType(),
                        IntegerType(),
                    ]
                )
            ],
            input_names=[lcol.upper() for lcol in LOAD_COLS],
            statement_params={"STATEMENT_TIMEOUT_IN_SECONDS": 7200},
            is_permanent=True,
        )
        class CircuitEncoderUDTF:
            def __init__(self):
                self.logger = logging.getLogger("CircuitEncoderUDTF_Logger")

            def end_partition(self, df: PandasDataFrame):
                network_transformer = SCECircuitDataTransformer()
                load_profile_controller = SCELoadProfileController()

                pipeline = SCEPowerflowPipelineTemp(
                    data_source,
                    transformer=network_transformer,
                    result_handler=result_handler,
                    load_profile_controller=load_profile_controller,
                )
                excluded_devices = ExcludedDevices()

                if not capacitor_enabled:
                    excluded_devices.disable_shunts()
                if not generator_enabled:
                    excluded_devices.disable_asymmetric_sgens()
                if not transformer_enabled:
                    excluded_devices.disable_transformers()
                if not regulator_enabled:
                    excluded_devices.disable_regulators()

                pipeline.set_excluded_devices(excluded_devices)                