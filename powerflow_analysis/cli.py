import os
import click
import pandas as pd
from snowflake.snowpark import Session
from .powerflow import CircuitAnalysis
from .data import SnowflakeDataSource, ExcelDataSource, get_table_lookup, create_snowflake_session, OUTPUT_COLUMNS
import pickle
import base64
from datetime import datetime


@click.group()
def cli():
    """Powerflow Analysis CLI"""
    pass

@cli.command()
@click.option('--circuit-key', required=True, help='Circuit key to download')
@click.option('--output', default='auto', help='Output Excel filefile; default is auto generated')
@click.option('--env', default='local', help='The Snowflake environment from which the data is being obtained, options are: local, dev, pt')
@click.option('--snowflake-config', type=click.Path(exists=True), default='snowflake_default.json', help='Path to Snowflake config JSON')
def download_circuit(circuit_key, output, snowflake_config, env):
    #TODO: maybe we want to add an option for specifiyng a load even in this command?
    if output == 'auto':
        output = f"{circuit_key}.xlsx"
    print(snowflake_config)
    """Download circuit data from Snowflake and save to Excel"""
    session = create_snowflake_session(snowflake_config)
    data_source = SnowflakeDataSource(session, get_table_lookup(env))  # Add table_lookup if needed
    circuit_data = data_source.load_circuit_data(circuit_key)
    ca = CircuitAnalysis(circuit_data, circuit_key)
    ca.save_to_excel(output)

    click.echo(f"Circuit data for {circuit_key} saved to {output}")

from datetime import datetime

@cli.command()
@click.option('--circuit-key', required=True, help='Circuit key to analyze')
@click.option('--start-time', required=True, default=datetime(year=2023, month=9, day=1, hour=12,minute=0, second=0), help='Start time for analysis')
@click.option('--end-time', required=True, default=datetime(year=2023, month=9, day=1, hour=13,minute=0, second=0), help='End time for analysis')
@click.option('--env', default='local', help='The Snowflake environment in which the analysis is being run, options are: local, dev, pt')
@click.option('--snowflake-config', default="snowflake_default.json", type=click.Path(exists=True), help='Path to Snowflake config JSON')
def run_from_snowflake(circuit_key, start_time, end_time, env, snowflake_config):
    session = create_snowflake_session(snowflake_config)
    table_lookup = get_table_lookup(env)
    data_source = SnowflakeDataSource(session, table_lookup)


@cli.command()
@click.option('--circuit-data', required=True, help='Circuit connectivity data to analyze (xlsx)', type=click.Path(exists=True))
@click.option('--load-data', required=True, help='Load data to analyze (csv)', type=click.Path(exists=True))
@click.option('--output-file', required=True, help='Name of file for output in csv')
def run_from_excel(circuit_data, load_data, output_file):
    load_data = pd.read_csv(load_data)
    load_data['reported_dttm'] = pd.to_datetime(load_data["reported_dttm"])
    circuit_data = ExcelDataSource(circuit_data).load_circuit_data()

    analysis = CircuitAnalysis(circuit_data,
                               capacitor_enabled=False,
                               generator_enabled=False, 
                               transformer_enabled=False,
                               balanced_load=False)
    analysis.analyze_period(load_data)
    results = analysis.circuit_data['RESULT'] 
    results.to_csv(output_file)

@cli.command()
@click.option('--input', required=True, type=click.Path(exists=True), help='Input CircuitAnalysis pickle file')
@click.option('--output', default='full_analysis_data.xlsx', help='Output Excel file')
def export_full_analysis(input, output):
    """Export all data from a completed CircuitAnalysis"""
    with open(input, 'rb') as f:
        analysis = pickle.load(f)
    
    with pd.ExcelWriter(output) as writer:
        # Export input data
        for sheet_name, df in analysis.grid.data_source.circuit_data.items():
            df.to_excel(writer, sheet_name=f"Input_{sheet_name}", index=False)
        
        # Export output data
        analysis.results.to_excel(writer, sheet_name="Results", index=False)
        
        # Export PandaPower diagnostic results
        pd.DataFrame(analysis.diagnostic_results).to_excel(writer, sheet_name="Diagnostics", index=False)
    
    click.echo(f"Full analysis data exported to {output}")


@cli.command()
@click.option('--package-path', default="stage_package_builds/latest/powerflow_analysis.zip", required=True, type=click.Path(exists=True), help='Path to the package file (.tar.gz or .whl)')
@click.option('--stage-name', default='powerflow_libs', help='Name of the Snowflake stage to upload to')
@click.option('--snowflake-config', type=click.Path(exists=True), default='snowflake_default.json', help='Path to Snowflake config JSON')
def upload_package(package_path, stage_name, snowflake_config):
    """Upload powerflow_analysis package to Snowflake stage"""
    session = create_snowflake_session(snowflake_config)
    
    # Get the filename from the package path
    package_filename = os.path.basename(package_path)
    #slightly gross 
    import json
    db = json.load(open(snowflake_config))["DB"]
    schema = json.load(open(snowflake_config))["SCHEMA"]
    try:
        session.file.put(package_path, f'@{db}.{schema}.{stage_name}', auto_compress=False, overwrite=True)
        click.echo(f"Successfully uploaded {package_filename} to @{stage_name}")
    except Exception as e:
        click.echo(f"Error uploading package: {str(e)}")
    finally:
        session.close()


#TODO: add options for start and end period, and also to specify which circuits are to be run;
#could be auto; could be a reference to a file, or could be a comma separated list of circuit keys
@cli.command()
@click.option('--env', type=click.Choice(['local', 'dev', 'pt'], case_sensitive=False), default='local', help='The Snowflake environment')
@click.option('--snowflake-config', type=click.Path(exists=True), default='snowflake_default.json', help='Path to Snowflake config JSON')
@click.option('--circuit-keys', default='auto', help='Comma separated list of circuit-keys to run; "auto" pulls all circuit ids from the hierarchy table')
@click.option('--output-dir', type=click.Path(), default='circuit_analyses', help='Directory to save Excel files')
@click.option('--capacitor/--no-capacitor', default=True, help='Enable/disable capacitor analysis')
@click.option('--generator/--no-generator', default=True, help='Enable/disable generator analysis')
@click.option('--transformer/--no-transformer', default=True, help='Enable/disable transformer analysis')
@click.option('--balanced-load/--unbalanced-load', default=False, help='Use balanced or unbalanced load')
@click.option('--debug/--no-debug', default=False, help='Enable/disable debug mode')
def batch_circuit_analysis(env, snowflake_config, circuit_keys, output_dir, capacitor, generator, transformer, balanced_load, debug):

    """Run diagnostics on multiple circuits and save results."""
    session = create_snowflake_session(snowflake_config)
    table_lookup = get_table_lookup(env)
    data_source = SnowflakeDataSource(session, table_lookup)

    start_period = datetime(2023,9,1,12,0,0)
    end_period = datetime(2023,9,1,13,0,0)

    if circuit_keys == "auto":
        # Query to get distinct circuit keys
        circuit_keys_df = session.sql(f"SELECT DISTINCT circuit_key FROM {table_lookup['HIERARCHY_TABLE']}").collect()
        circuit_keys = [row['CIRCUIT_KEY'] for row in circuit_keys_df]
    else:
        circuit_keys = [c.strip() for c in circuit_keys.split(',')]
    os.makedirs(output_dir, exist_ok=True)
    
    powerflow_output_dataframes = []
    results = []
    for circuit_key in circuit_keys:
        click.echo(f"Processing circuit: {circuit_key}: CAP: {capacitor}\t GEN: {generator}\t TRAFO: {transformer}\t Balanced: {balanced_load}")
        row = {"circuit_key": circuit_key, "notes":""}
        try:
            # Create CircuitAnalysis object
            circuit_analysis = CircuitAnalysis.from_snowflake(session, table_lookup, circuit_key,
                                                              capacitor_enabled=capacitor,
                                                              generator_enabled=generator,
                                                              transformer_enabled=transformer,
                                                              balanced_load=balanced_load
                                                              )
            row["status"] = "created grid"

            powerflow_out = circuit_analysis.analyze_period(data_source.load_time_series_data(circuit_key, start_period, end_period))
            powerflow_output_dataframes.append(powerflow_out)

            row["status"] = "ran powerflow"
            # Run diagnostics
            circuit_analysis.run_diagnostic()
            
            # Save to Excel
            excel_path = os.path.join(output_dir, f"{circuit_key}_analysis.xlsx")
            circuit_analysis.save_to_excel(excel_path)
            click.echo(f"Saved Excel file: {excel_path}")
            row["status"] = "complete"

            circuit_analysis.generate_html_report(output_dir)
            ## Save to Snowflake
            #circuit_analysis.save_to_snowflake(data_source)
            #click.echo(f"Saved to Snowflake: {circuit_key}")

        except Exception as e:
            if debug:
                raise
            click.echo(f"Error processing circuit {circuit_key}: {str(e)}")
            row["status"] = "error"
            row["notes"] = str(e)
        results.append(row)
    pd.DataFrame.from_records(results).to_csv('batch_summary.csv')
    click.echo("Batch circuit analysis completed. Summary saved to batch_summary.csv")
    all_out = pd.concat(powerflow_output_dataframes)
    all_out.to_csv('all_powerflow_out.csv')
    #session.sql(f"use database {table_lookup['SNOWFLAKE_DB_FERC']}")
    #session.create_dataframe(all_out).select(OUTPUT_COLUMNS).write().mode('append').save_as_table(table_lookup['OUTPUT_TABLE'])




if __name__ == '__main__':
    cli()