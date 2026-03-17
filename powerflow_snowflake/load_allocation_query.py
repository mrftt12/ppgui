from powerflow_pipeline.util import PowerflowConfig

class LoadAllocationQuery():
    def __init__(self, pf_config, circuit_key: str, start_time_dt: str, end_time_dt: str, run_tag: str):
        self.circuit_key = circuit_key
        self.run_tag = run_tag
        self.pf_config: PowerflowConfig = pf_config
        self.start_time_dt = start_time_dt
        self.end_time_dt = end_time_dt

    def parameters(self):
        params = []
        params.append(self.circuit_key)
        params.append(self.start_time_dt)
        params.append(self.end_time_dt)
        return params


    def sql(self):
        # date_filter = (f"AND REPORTED_DTTM = '{self.start_time_dt}'" if self.start_time_dt == self.end_time_dt
        #                else f"AND REPORTED_DTTM >= '{self.start_time_dt}' AND REPORTED_DTTM <= '{self.end_time_dt}'")
        return f"""
with ReadingData as (select
power_system_resource_mrid, power_system_resource_key, reported_dttm, YEAR_ID, MONTH_ID,
ARRAY_AGG(
OBJECT_CONSTRUCT(
    'NAME',
    POWER_SYSTEM_RESOURCE_NUM::VARCHAR,
    'RESOURCE_TYPE',
    POWER_SYSTEM_RESOURCE_TYPE,
    'MEASURE_VALUE',
    MEASURE_VALUE_01,
    'MEASURE_VALUE_A',
    MEASURE_VALUE_A_01,
    'MEASURE_VALUE_B',
    MEASURE_VALUE_B_01,
    'MEASURE_VALUE_C',
    MEASURE_VALUE_C_01,
    'RDNG_MEAS_MVAR',
    MEASURE_VALUE_02,
    'RDNG_MEAS_MVAR_A',
    MEASURE_VALUE_A_02,
    'RDNG_MEAS_MVAR_B',
    MEASURE_VALUE_B_02,
    'RDNG_MEAS_MVAR_C',
    MEASURE_VALUE_C_02,
    'UNIT_SYMBOL',
    UNIT_SYMBOL,
    'MEASUREMENT_TYPE',
    MEASUREMENT_TYPE,
    'POWER_SYSTEM_RESOURCE_TYPE',
    POWER_SYSTEM_RESOURCE_TYPE,
    'PROFILE_TYPE',
    PROFILE_TYPE,
    'REPORTED_DT',
    REPORTED_DT,
    'POWER_SYSTEM_RESOURCE_NUM',
    POWER_SYSTEM_RESOURCE_NUM
)
) AS READINGS
from {self.pf_config.get_load_allocation_view_database()}.{self.pf_config.get_load_allocation_view_schema()}.{self.pf_config.get_load_allocation_view()} 
where
power_system_resource_key = ? and profile_state = 'INITIAL'
AND REPORTED_DTTM >= ? AND REPORTED_DTTM <= ?
group by power_system_resource_mrid, power_system_resource_key, reported_dttm, YEAR_ID, MONTH_ID)
select POWER_SYSTEM_RESOURCE_MRID, POWER_SYSTEM_RESOURCE_KEY, REPORTED_DTTM::TIMESTAMP_NTZ(9) AS REPORTED_DTTM, YEAR_ID, MONTH_ID, READINGS,
ca.preload_encoded_ca CA_BLOB,
OBJECT_CONSTRUCT(
    'EDW_CREATED_DATE', TO_CHAR(SYSDATE(), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'),
    'EDW_CREATED_BY', TO_CHAR(CURRENT_USER()),
    'EDW_MODIFIED_DATE', TO_CHAR(SYSDATE(), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'),
    'EDW_MODIFIED_BY', TO_CHAR(CURRENT_USER()),
    'EWD_BATCH_ID', TO_CHAR(DATE_PART(epoch_microsecond, CURRENT_TIMESTAMP())),
    'EWD_BATCH_DETAIL_ID', TO_CHAR(DATE_PART(epoch_microsecond, CURRENT_TIMESTAMP())),
    'EDW_LAST_DML_CD', 'I'
)::VARCHAR AS EXTRA_METADATA
from
ReadingData rd join 
{self.pf_config.get_database()}.{self.pf_config.get_database_schema()}.{self.pf_config.get_encoded_circuits_table()} ca 
ON rd.power_system_resource_key = ca.circuit_key
"""