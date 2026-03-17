import os
import re
import uuid
import glob
from datetime import datetime, timedelta, date
import snowflake.connector
from snowflake.connector.cursor import SnowflakeCursor


BASE_CIRCUIT_KEY = "CKT_1770_16570"
NUM_RECORDS_TO_GENERATE = 100

exclusions = ['CKT_898_15340', 'CKT_2475_07240', 'CKT_1224_15922', 'CKT_5848_00812', 'CKT_1161_13164', 'CKT_2926_09840', 'CKT_869_15150', 'CKT_379_14544', 'CKT_3080_07928', 'CKT_3324_05204', 'CKT_3976_01626', 'CKT_3509_08666', 'CKT_2405_09355', 'CKT_3816_09619', 'CKT_4186_02037', 'CKT_3150_03903', 'CKT_14611_06530', 'CKT_8860_18163', 'CKT_2080_11920', 'CKT_4356_13518', 'CKT_1313_16007', 'CKT_361_15376', 'CKT_4019_10074', 'CKT_1330_16073', 'CKT_1106_15646', 'CKT_2751_02720', 'CKT_6040_02763', 'CKT_6056_16003', 'CKT_2532_03270', 'CKT_2839_03328', 'CKT_1108_15648', 'CKT_1547_16352', 'CKT_3466_02876', 'CKT_2869_08698', 'CKT_3649_03356', 'CKT_1776_16585', 'CKT_3682_06287', 'CKT_167_03257', 'CKT_2879_09877', 'CKT_4937_08939', 'CKT_2186_12523', 'CKT_522_14661', 'CKT_3240_02403', 'CKT_1858_15372', 'CKT_3168_04526', 'CKT_3_19809', 'CKT_89_16773', 'CKT_1459_18657', 'CKT_2306_03990', 'CKT_3360_08343', 'CKT_1879_16763', 'CKT_2858_03000', 'CKT_4113_01876', 'CKT_2533_03515', 'CKT_14606_11476', 'CKT_2132_12175', 'CKT_3917_04085', 'CKT_5220_18631', 'CKT_242_16890', 'CKT_5322_13201', 'CKT_165_16112', 'CKT_3930_06987', 'CKT_1878_14004', 'CKT_1911_00726', 'CKT_1213_15865', 'CKT_4276_04885', 'CKT_2832_03301', 'CKT_1429_13580', 'CKT_4906_05147', 'CKT_2353_02370', 'CKT_2586_10620', 'CKT_18_03353', 'CKT_14974_11641', 'CKT_2315_04795', 'CKT_3109_08662', 'CKT_329_12167', 'CKT_4110_01849', 'CKT_2021_01251', 'CKT_3245_02422', 'CKT_2416_02730', 'CKT_5777_03296', 'CKT_3856_06717', 'CKT_356_11512', 'CKT_5413_14406', 'CKT_2474_05400', 'CKT_2079_11915', 'CKT_1962_01050', 'CKT_4425_12226', 'CKT_3028_10368', 'CKT_4122_04504', 'CKT_1637_14047', 'CKT_9434_08216', 'CKT_2803_01282', 'CKT_2479_04530', 'CKT_3459_11685', 'CKT_2023_01255', 'CKT_2443_03688', 'CKT_3745_06429', 'CKT_3458_11675', 'CKT_2502_11554', 'CKT_5795_05061', 'CKT_1727_14043', 'CKT_1695_19150', 'CKT_2584_08020', 'CKT_979_15408', 'CKT_4907_17033', 'CKT_3613_06014', 'CKT_2940_02560', 'CKT_3776_03715', 'CKT_2947_06280', 'CKT_277_04586', 'CKT_4801_17982', 'CKT_426_17190', 'CKT_4892_11166', 'CKT_2956_10705', 'CKT_4413_06666', 'CKT_3761_09429', 'CKT_3582_08905', 'CKT_2135_12198', 'CKT_3586_08916', 'CKT_1787_16600', 'CKT_2966_01837', 'CKT_600_17690', 'CKT_3088_01604', 'CKT_1221_15910', 'CKT_1809_19340', 'CKT_2063_11843', 'CKT_14899_11561', 'CKT_2948_07960', 'CKT_5327_08298', 'CKT_1815_19373', 'CKT_1051_12960', 'CKT_1762_14178', 'CKT_2822_03510', 'CKT_1770_16570', 'CKT_3642_11744', 'CKT_1729_14074', 'CKT_734_00032', 'CKT_3250_02485', 'CKT_2925_06760', 'CKT_1842_19300', 'CKT_1398_13680', 'CKT_4031_10180', 'CKT_2039_00814', 'CKT_389_14580', 'CKT_4295_07851', 'CKT_2578_08860', 'CKT_3432_08519', 'CKT_2382_06504', 'CKT_4743_00889', 'CKT_9326_11731', 'CKT_2673_08810', 'CKT_3498_05591', 'CKT_2653_07530', 'CKT_1909_00720', 'CKT_1689_19139', 'CKT_1806_19309', 'CKT_3091_06273', 'CKT_3144_06035', 'CKT_3260_05085', 'CKT_4753_01515', 'CKT_2937_06730', 'CKT_442_02690', 'CKT_14900_11751', 'CKT_3206_01354', 'CKT_3517_08697', 'CKT_4894_02421', 'CKT_1561_16404', 'CKT_1029_18017', 'CKT_3849_03935', 'CKT_36_03319', 'CKT_9102_07446', 'CKT_2595_01360', 'CKT_2881_11015', 'CKT_412_17090', 'CKT_2_10191', 'CKT_2965_07560', 'CKT_1897_00670', 'CKT_2200_12600', 'CKT_5024_17059', 'CKT_2267_10417', 'CKT_3022_10147', 'CKT_2361_02510', 'CKT_1824_19417', 'CKT_1620_13959', 'CKT_3597_03263', 'CKT_2219_12716', 'CKT_1277_13467', 'CKT_2961_05200', 'CKT_2620_06844', 'CKT_3097_07320', 'CKT_133_19655', 'CKT_775_15051', 'CKT_114_16955', 'CKT_436_17256', 'CKT_3328_05211', 'CKT_4854_12781', 'CKT_2354_05310', 'CKT_421_17140', 'CKT_3077_04226', 'CKT_1203_15737', 'CKT_1578_18929', 'CKT_3386_11446', 'CKT_3983_04115', 'CKT_2465_10435', 'CKT_3312_02562', 'CKT_1393_13655', 'CKT_4075_07385', 'CKT_4888_17797', 'CKT_2082_11931', 'CKT_1255_18335', 'CKT_1014_17972', 'CKT_4082_07455', 'CKT_4145_07568', 'CKT_2342_01960', 'CKT_2567_09430', 'CKT_745_00050', 'CKT_590_06078', 'CKT_2707_07520', 'CKT_4858_04053', 'CKT_2993_11204', 'CKT_4380_03370', 'CKT_3162_10610', 'CKT_1427_16147', 'CKT_137_19697', 'CKT_5052_17354', 'CKT_2340_10500', 'CKT_584_01000', 'CKT_170_07251', 'CKT_4372_05219', 'CKT_3774_03686', 'CKT_3183_03210', 'CKT_4837_07109', 'CKT_2055_00878', 'CKT_904_17820', 'CKT_1636_14037', 'CKT_2994_08460', 'CKT_1093_15603', 'CKT_2503_11780', 'CKT_1492_13758', 'CKT_2380_07900', 'CKT_1502_13795', 'CKT_3759_09420', 'CKT_1248_18307', 'CKT_2414_04770', 'CKT_1228_15953', 'CKT_2410_09443', 'CKT_3175_07570', 'CKT_715_17604', 'CKT_4150_07603', 'CKT_2841_08408', 'CKT_3315_02578', 'CKT_3869_09718', 'CKT_4053_04314', 'CKT_1957_00990', 'CKT_3158_04772', 'CKT_3176_07820', 'CKT_5902_19838', 'CKT_3472_02915', 'CKT_3347_08256', 'CKT_2150_12317', 'CKT_313_13175', 'CKT_2290_08949', 'CKT_3740_06361', 'CKT_4176_01945', 'CKT_1174_13256', 'CKT_1245_18295', 'CKT_3512_08689', 'CKT_1854_10333', 'CKT_4807_11184', 'CKT_2969_07280', 'CKT_14460_19586', 'CKT_2339_03090', 'CKT_2880_08420', 'CKT_2547_05819', 'CKT_1173_13253', 'CKT_749_00091', 'CKT_3872_09781', 'CKT_2924_01680', 'CKT_706_17558', 'CKT_922_17915', 'CKT_3387_11448', 'CKT_2991_07134', 'CKT_1905_00695', 'CKT_2594_06040', 'CKT_4427_06169', 'CKT_4756_10551', 'CKT_4795_00436', 'CKT_4778_16806', 'CKT_2309_07370', 'CKT_1507_13820', 'CKT_3209_06888', 'CKT_1658_16472', 'CKT_1550_16359', 'CKT_768_15010', 'CKT_2652_04660', 'CKT_4814_08651', 'CKT_1532_16308', 'CKT_4361_16944', 'CKT_4818_14396', 'CKT_2859_08480', 'CKT_3462_02847', 'CKT_4175_01940', 'CKT_73_14341', 'CKT_4727_14925', 'CKT_4217_07670', 'CKT_589_03838', 'CKT_8682_02674', 'CKT_4328_10872', 'CKT_841_00331', 'CKT_5670_00020', 'CKT_501_00245', 'CKT_2885_06065', 'CKT_969_12910', 'CKT_2417_04000', 'CKT_4102_01822', 'CKT_3309_02531', 'CKT_3227_10951', 'CKT_3484_02965', 'CKT_4382_11350', 'CKT_5150_06595', 'CKT_2476_08240', 'CKT_2292_10360', 'CKT_3081_08479', 'CKT_2955_09450', 'CKT_274_19772', 'CKT_3263_05115', 'CKT_4090_10261', 'CKT_3362_08363', 'CKT_1621_13960', 'CKT_2600_02855', 'CKT_5073_02672', 'CKT_2882_11364', 'CKT_1893_00663', 'CKT_1742_14134', 'CKT_2811_11341', 'CKT_2301_08405', 'CKT_2581_01865', 'CKT_14671_09092', 'CKT_5371_03885', 'CKT_4355_03727', 'CKT_14848_06201', 'CKT_3447_11535', 'CKT_3796_06549', 'CKT_2109_12070', 'CKT_5065_01851', 'CKT_3188_05327', 'CKT_443_05088', 'CKT_423_17158', 'CKT_2458_02424', 'CKT_156_06131', 'CKT_14781_09735', 'CKT_1209_15758', 'CKT_3892_01371', 'CKT_196_19261', 'CKT_226_14436', 'CKT_3090_02762']

exclusions_set = set(exclusions)

def get_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            # warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            warehouse="TEST_SMALL",
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
        )
        return conn
    except snowflake.connector.errors.Error as e:
        print(f"Error connecting to Snowflake: {e}")
        print("Please ensure all SNOWFLAKE_* environment variables are set correctly.")
        return None


def get_profile_data_files(start_path):
    files = []
    for dirpath, dirnames, filenames in os.walk(start_path):
        if filenames:
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                parent_folder = os.path.basename(os.path.normpath(dirpath))
                if parent_folder == "profile" and (
                    filename.endswith(".parquet") or filename.endswith(
                        ".parquet.gzip")
                ):
                    files.append(file_path)
    return files


def process_historical_load_data(conn: snowflake.connector.SnowflakeConnection):
    target_table = "GRIDMOD_DEV_TD.UC_POC.NMM_F_HIST_HR_GROSS_DERGEN_DLY_C_PP_VW_MC9"
    stage_name = f"load_test_stage_{uuid.uuid4().hex}"

    try:
        parquet_files = get_profile_data_files("tmp/circuit_and_profile_data")
        if not parquet_files:
            print(f"No Parquet files found matching.")
            return

        with conn.cursor() as cur:
            print(f"Creating temporary stage: {stage_name}")
            cur.execute(
                f"CREATE OR REPLACE TEMPORARY STAGE {stage_name} FILE_FORMAT = (TYPE = PARQUET)"
            )

            for file_path in parquet_files:
                # Uploading tmp/circuit_and_profile_data/tmp/circuit_and_profile_data/CKT_1228_15953/profile/CKT_1228_15953_profile_9-2025.parquet.gzip

                match = re.search(r'\b(CKT_\d+_\d+)\b', file_path)
                if match:
                    extracted_id = match.group(1)
                    print(extracted_id)
                    if extracted_id in exclusions_set:
                        continue
                else:
                    print("Circuit ID pattern not found in the string.")

                print(f"Uploading {file_path} to stage {stage_name}...")
                abs_path = os.path.abspath(file_path)
                put_command = f"PUT file://{abs_path} @{stage_name}"
                cur.execute(put_command)
                print(f"File {file_path} uploaded successfully.")

                print(f"Copying data from stage into {target_table}...")
                # When loading from Parquet, timestamp columns with nanosecond precision
                # (which Snowflake reads as TIMESTAMP_NTZ(9)) can cause an error if the
                # target column has a lower precision (e.g., TIMESTAMP_NTZ(6)).
                # The Parquet files contain timestamps as numeric epoch values in nanoseconds.
                # The `::` cast operator does not work on numeric epoch values.
                # We must use the TO_TIMESTAMP_NTZ function. The division by 1000 was
                # creating a floating-point number, which is an invalid type for the function.
                # We now cast the result of the division to BIGINT to ensure it's an integer
                # before passing it to the timestamp conversion function.
                copy_sql = f"""
                    COPY INTO {target_table}
                    FROM (
                        SELECT
                            $1:POWER_SYSTEM_RESOURCE_MRID,
                            $1:POWER_SYSTEM_RESOURCE_KEY,
                            $1:STRUCT_NUM,
                            TO_TIMESTAMP_NTZ(($1:REPORTED_DTTM / 1000)::BIGINT, 6),
                            $1:REPORTED_DT,
                            $1:MEASURE_VALUE,
                            $1:MEASURE_VALUE_A,
                            $1:MEASURE_VALUE_B,
                            $1:MEASURE_VALUE_C,
                            $1:RDNG_MEAS_MVAR,
                            $1:RDNG_MEAS_MVAR_A,
                            $1:RDNG_MEAS_MVAR_B,
                            $1:RDNG_MEAS_MVAR_C,
                            $1:UNIT_SYMBOL,
                            $1:MEASUREMENT_TYPE,
                            $1:PROJECT_ID,
                            $1:TECH_TYPE,
                            $1:POWER_SYSTEM_RESOURCE_TYPE,
                            $1:YEAR_ID,
                            $1:MONTH_ID,
                            TO_TIMESTAMP_NTZ(($1:EDW_CREATED_DATE / 1000)::BIGINT, 6),
                            $1:EDW_CREATED_BY,
                            TO_TIMESTAMP_NTZ(($1:EDW_MODIFIED_DATE / 1000)::BIGINT, 6),
                            $1:EDW_MODIFIED_BY,
                            $1:EDW_BATCH_ID,
                            $1:EDW_BATCH_DETAIL_ID,
                            $1:EDW_LAST_DML_CD
                        FROM @{stage_name}
                    )
                """
                cur.execute(copy_sql)
                print(
                    f"Successfully copied {cur.rowcount} records into {target_table}."
                )

    except snowflake.connector.errors.Error as e:
        print(f"An error occurred during Parquet ingestion: {e}")
    except Exception as e:
        print(f"A general error occurred: {e}")
    finally:
        try:
            with conn.cursor() as cur:
                print(f"Dropping stage {stage_name}...")
                cur.execute(f"DROP STAGE IF EXISTS {stage_name}")
                print("Stage dropped.")
        except snowflake.connector.errors.Error as e:
            print(
                f"Could not drop stage {stage_name}. Manual cleanup may be required. Error: {e}"
            )


def main():
    conn = get_snowflake_connection()
    if conn:
        try:
            process_historical_load_data(conn)

        finally:
            conn.close()
    else:
        print("Could not establish Snowflake connection. Exiting.")


if __name__ == "__main__":
    main()
