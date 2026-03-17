import pickle
import snowflake.snowpark

import types
import multiconductor as mc

from powerflow_pipeline.powerflow import ExcludedDevices, Pipeline, ResultHandler
from powerflow_pipeline.util import PowerflowConfig
from powerflow_snowflake.ami_load_allocation_wrapper import AMILoadAllocation
from powerflow_snowflake.batch_runner import SnowflakePowerflowRunner
from powerflow_snowflake.circuit_cache_builder import SnowflakeCircuitCacheManager
from powerflow_snowflake.load_allocation_wrapper import LoadAllocation
from sce.load_allocation import SCELoadAllocationMeasurement
from sce.sce_mc_mapping import SCECircuitDataTransformer, SCELoadProfileController, SCEResultsTransformer, SCEPowerflowPipelineTemp, SnowflakeDataSource
import logging

from .circuit_encoder import CircuitEncoder

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 
def run_load_allocation(
    session: snowflake.snowpark.Session,
    pf_config_json: str,
    start_time: str,
    end_time: str,
    circuit_keys: str,
    procedure: str,
):
    if procedure == 'CC_ALLOCATION':
        load_allocation = LoadAllocation(
            session,
            pf_config_json,
            start_time,
            end_time,
            circuit_keys
        )
        run_tag = load_allocation.run_load_allocation()
    elif procedure == 'AMI_ALLOCATION':
        load_allocation = AMILoadAllocation(
            session,
            pf_config_json,
            start_time,
            end_time,
            circuit_keys
        )
        run_tag = load_allocation.run_load_allocation()
    return (run_tag)

def run_powerflow(
    session: snowflake.snowpark.Session,
    pf_config_json: str,
    start_time: str,
    end_time: str,
    circuit_keys: str,
    circuit_size: str,
):
    runner = SnowflakePowerflowRunner(
        session,
        pf_config_json,
        start_time,
        end_time,
        circuit_keys,
        circuit_size,
    )

    run_tag = runner.run_pf_batches()
    return (run_tag)


def define_circuit_parallel(
    session: snowflake.snowpark.Session,
    circuit_key: str,
    pf_config_json: str,
    capacitor_enabled: bool,
    generator_enabled: bool,
    transformer_enabled: bool,
    regulator_enabled: bool,
) -> str:
    circuit_keys = [c.strip() for c in circuit_key.split(',')]
    pf_config = PowerflowConfig()
    pf_config.load_config_from_json(pf_config_json)
    logger.info(f"define_circuit::multiconductor version: {mc.__version__}")
    circuit_encoder = CircuitEncoder(session=session, pf_config=pf_config, circuit_keys=circuit_key, capacitor_enabled=capacitor_enabled,
                                     generator_enabled=generator_enabled, transformer_enabled=transformer_enabled, regulator_enabled=regulator_enabled)
    circuit_encoder.encode_circuits()


def define_circuit(
    session: snowflake.snowpark.Session,
    circuit_key: str,
    pf_config_json: str,
    capacitor_enabled: bool,
    generator_enabled: bool,
    transformer_enabled: bool,
    regulator_enabled: bool,
) -> str:
    circuit_keys = [c.strip() for c in circuit_key.split(',')]
    pf_config = PowerflowConfig()
    pf_config.load_config_from_json(pf_config_json)
    logger.info(f"define_circuit::multiconductor version: {mc.__version__}")

    result_handler = ResultHandler(result_transformer=SCEResultsTransformer())
    for ck in circuit_keys:
        data_source = SnowflakeDataSource(
            session,
            pf_config.get_database(),
            pf_config.get_database_schema(),
            pf_config.get_bus_table_database(),
            pf_config.get_bus_table_schema(),
            pf_config.get_connectivity_table_database(),
            pf_config.get_connectivity_table_schema(),
            pf_config.get_bus_table(),
            pf_config.get_connectivity_table(),
            ck,
        )

        network_transformer = SCECircuitDataTransformer()
        load_profile_controller = SCELoadProfileController()
        load_allocation_meas = SCELoadAllocationMeasurement()

        pipeline = SCEPowerflowPipelineTemp(
            data_source,
            transformer=network_transformer,
            load_allocation_measurement=load_allocation_meas,
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

        sf_cache_manager = SnowflakeCircuitCacheManager(session, pf_config)
        logger.info(f"Generating net for circuit {ck}")
        sf_cache_manager.put_circuit_object(
            ck,
            pipeline
        )

    return f"SUCCESS ({len(circuit_keys)})"
