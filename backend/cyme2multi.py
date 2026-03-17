import subprocess
import sqlite3
import pandas as pd
import io
import os
import sys

# Configuration
MDB_PATH = "TCGMCYME.mdb"
DB_OUTPUT = "cyme2multi.db"

# User provided list with corrections mapping
TABLE_MAPPING = {
    # Network Information
    "CYMNODE": "CYMNODE",
    "CYMSECTION": "CYMSECTION",
    "CYMLOAD": "CYMLOAD",
    "CYMMETER": "CYMMETER",
    "CYMNETWORK": "CYMNETWORK",
    "CYMOVEHEADBYPHASE": "CYMOVERHEADBYPHASE",
    "CYMOVERHEADLINE": "CYMOVERHEADLINE",
    "CYMSECTIONDEVICE": "CYMSECTIONDEVICE",
    "CYMSHUNTCAPACITOR": "CYMSHUNTCAPACITOR",
    "CYMEQUIVALENTSOURCE": "CYMEQUIVALENTSOURCE",
    "CYMSYNCHRONOUSGENERATOR": "CYMSYNCHRONOUSGENERATOR",
    "CYMTRANSFORMER": "CYMTRANSFORMER",
    "CYMTRANSFORMERBYPHASE": "CYMTRANSFORMERBYPHASE",
    "CYMTHREEWINDINGTRANSFORMER": "CYMTHREEWINDINGTRANSFORMER",
    "CYMREGULATOR": "CYMREGULATOR",
    "CYMRECLOSER": "CYMRECLOSER",
    "CYMFUSE": "CYMFUSE",
    "CYMSWITCH": "CYMSWITCH",
    "CYMPHOTOVOLTAIC": "CYMPHOTOVOLTAIC",
    "CYMUNDERGROUNDLINE": "CYMUNDERGROUNDLINE",
    "CYMCUSTOMERLOAD": "CYMCUSTOMERLOAD",
    "CYMCONSUMERCLASS": "CYMCONSUMERCLASS",  # Sometimes related to generic loads
    # Equipment Library Data
    # Equipment Library Data
    "CYMEQCABLE": "CYMEQCABLE",
    "CYMEQCABLECONCENTRICNEUTRAL": "CYMEQCABLECONCENTRICNEUTRAL",
    "CYMECABLECONDUCTOR": "CYMEQCABLECONDUCTOR",
    "CYMEQCABLEINSULATION": "CYMEQCABLEINSULATION",
    "CYMEQCABLESHEATH": "CYMEQCABLESHEATH",
    "CYMEQCHARGER": "CYMEQCHARGER",
    "CYMEQCONVERTERGENERATOR": "CYMEQELECCONVERTERGENERATOR",
    "CYMEQFUSE": "CYMEQFUSE",
    "CYMEQGEOMETRICALARRANGEMENT": "CYMEQGEOMETRICALARRANGEMENT",
    "CYMEQINDUCTIONGENERATOR": "CYMEQINDUCTIONGENERATOR",
    "CYMEQINDUCTIONMACHINEQCIRCUIT": "CYMEQINDUCTIONMACHINEEQCIRCUIT",
    "CYMEQINDUCTIONMOTOR": "CYMEQINDUCTIONMOTOR",
    "CYMEQLOADTAPCHANGER": "CYMEQLOADTAPCHANGER",
    "CYMEQOVERHEADLINE": "CYMEQOVERHEADLINE",
    "CMEQOVERHEADLINEUNBALANCED": "CYMEQOVERHEADLINEUNBALANCED",
    "CYMEQOVERHEADSPACINGOFCOND": "CYMEQOVERHEADSPACINGOFCOND",
    "CYMEQPHOTOVOLTAIC": "CYMEQPHOTOVOLTAIC",
    "CYMEQRECLOSER": "CYMEQRECLOSER",
    "CYMEQREGULATOR": "CYMEQREGULATOR",
    "CYMEQRELIABILITYEXTENSION": "CYMEQRELIABILITYEXTENSION",
    "CYMEQSHUNTCAPACITOR": "CYMEQSHUNTCAPACITOR",
    "CYMEQSOURCE": "CYMEQSOURCE",
    "CYMEQSWITCH": "CYMEQSWITCH",
    "CYMEQSYNCHRONOUSGENERATOR": "CYMEQSYNCHRONOUSGENERATOR",
    "CYMEQTRANSFORMER": "CYMEQTRANSFORMER",
}


def get_new_table_name(old_name):
    if old_name == "CYMEQUIVALENTSOURCE":
        return "MULTISOURCE"
    if old_name == "CYMSYNCHRONOUSGENERATOR":
        return "MULTISYNCHRONOUSGENERATOR"
    if old_name == "CYMTRANSFORMER":
        return "MULTITRANSFORMER"
    if old_name == "CYMTRANSFORMERBYPHASE":
        return "MULTITRANSFORMERBYPHASE"
    if old_name == "CYMCUSTOMERLOAD":
        return "MULTICUSTOMERLOAD"
    if old_name == "CYMREGULATOR":
        return "MULTIREGULATOR"
    if old_name == "CYMRECLOSER":
        return "MULTIRECLOSER"
    if old_name == "CYMFUSE":
        return "MULTIFUSE"
    if old_name == "CYMSWITCH":
        return "MULTISWITCH"
    if old_name == "CYMTHREEWINDINGTRANSFORMER":
        return "MULTITHREEWINDINGTRANSFORMER"
    if old_name == "CYMPHOTOVOLTAIC":
        return "MULTIPHOTOVOLTAIC"
    if old_name == "CYMUNDERGROUNDLINE":
        return "MULTIUNDERGROUNDLINE"

    # Rule: Replace CYM/CYME prefix with MULTI
    # Based on checking, simple CYM -> MULTI works for both CYM... and CYMEQ...
    if old_name.startswith("CYM"):
        return "MULTI" + old_name[3:]
    return old_name  # Should not happen based on list


def export_and_load():
    if not os.path.exists(MDB_PATH):
        print(f"Error: {MDB_PATH} not found.")
        return

    # Remove existing DB if needed, or append? better to replace for clean migration
    if os.path.exists(DB_OUTPUT):
        os.remove(DB_OUTPUT)
        print(f"Removed existing {DB_OUTPUT}")

    conn = sqlite3.connect(DB_OUTPUT)

    # Get actual tables from MDB to verify existence
    r = subprocess.run(["mdb-tables", "-1", MDB_PATH], capture_output=True, text=True)
    if r.returncode != 0:
        print("Error checking MDB tables")
        return
    actual_tables = set(r.stdout.splitlines())

    for user_ref, actual_name in TABLE_MAPPING.items():
        if actual_name not in actual_tables:
            print(
                f"Warning: Table {actual_name} (ref: {user_ref}) not found in MDB. Skipping."
            )
            continue

        print(f"Processing {actual_name}...")

        # Export to CSV
        # We use subprocess to stream the output directly to pandas
        proc = subprocess.Popen(
            ["mdb-export", MDB_PATH, actual_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            # Read CSV
            df = pd.read_csv(proc.stdout)

            # Determine new name
            new_name = get_new_table_name(actual_name)

            # Save to SQLite
            df.to_sql(new_name, conn, if_exists="replace", index=False)
            print(f"  -> Saved as {new_name} ({len(df)} rows)")

        except Exception as e:
            print(f"  Error processing {actual_name}: {e}")
        finally:
            proc.stdout.close()
            proc.wait()

    conn.close()
    print("Migration complete.")


if __name__ == "__main__":
    export_and_load()
