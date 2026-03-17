import pickle
import snowflake.snowpark

import types

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_powerflow(
    session: snowflake.snowpark.Session,
    pf_config_json: str,
    start_time: str,
    end_time: str,
    circuit_keys: str,
    circuit_size: str,
):

    return "SUCCESS"


def define_circuit(
    session: snowflake.snowpark.Session,
    circuit_key: str,
    pf_config_json: str,
    capacitor_enabled: bool,
    generator_enabled: bool,
    transformer_enabled: bool,
    regulator_enabled: bool,
) -> str:

    return "SUCCESS"
