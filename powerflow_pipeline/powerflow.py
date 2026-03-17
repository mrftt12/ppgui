from dataclasses import dataclass
from enum import Enum
import pickle
import random
import string
import time
import warnings
import pandas as pd
import pandapower as pp
from abc import ABC, abstractmethod
import multiconductor as mc
import pandapower as pp

from .util import track_time, validate
import logging

logging.basicConfig(level=logging.INFO)


class DataSource(ABC):
    @abstractmethod
    def retrieve(self) -> dict:
        """Retrieve raw network data as a dictionary of DataFrames"""
        pass


class MulticonductorExcelDataSource(DataSource):
    def __init__(self, file_path: str):
        self.file_path = file_path

    def retrieve(self) -> dict:
        self.dataframes = mc.from_excel(self.file_path)
        return self.dataframes


class NetworkDataTransformer(ABC):
    @abstractmethod
    def transform(self, raw_data: dict) -> tuple[pp.pandapowerNet, dict]:
        """Transform raw data dictionary into pandapower network"""
        pass


class NoOpDataTransformer(NetworkDataTransformer):
    def transform(self, net: dict) -> tuple[pp.pandapowerNet, dict]:
        return (net, {})

class LoadProfileController(ABC):
    @abstractmethod
    def update_step(self, net: pp.pandapowerNet, load_data: any):
        pass

class ResultTransformer(ABC):
    @abstractmethod
    def transform(self, net: pp.pandapowerNet, load_data: any = None) -> any:
        pass


class ResultWriter(ABC):
    @abstractmethod
    def write(self, output: any):
        pass


class ResultHandler:
    def __init__(
        self,
        result_transformer: ResultTransformer = None,
        result_writer: ResultWriter = None,
    ):
        self.result_transformer = result_transformer
        self.result_writer = result_writer

    def apply(self, result: pp.pandapowerNet, load_data:any = None):
        final_output = result
        if self.result_transformer:
            final_output = self.result_transformer.transform(result, load_data)

        if self.result_writer:
            self.result_writer.write(final_output)

        return final_output


class VoltageReportTransformer(ResultTransformer):
    def transform(self, net: pp.pandapowerNet, load_data: any = None) -> pd.DataFrame:
        return net.res_bus[["vm_pu", "va_degree"]].reset_index()


class SimpleResultsTransformer(ResultTransformer):
    def transform(self, net: pp.pandapowerNet, load_data: any = None) -> dict:
        return {
            "buses": net.res_bus,
            "lines": net.res_line,
            "transformers": getattr(net, "res_trafo", None),
            "converged": net.converged,
        }


def validate_exlusion_arg(*args, **kwargs):
    if len(args) > 2:
        raise ValueError(f"Expecting zero or one argument, got {args}")

    if len(args) == 2 and not isinstance(args[1], DeviceSelector):
        raise ValueError(f"Expecting DeviceSelector, got {args}")


@dataclass
class SelectCriteria(Enum):
    ALL = 1
    BY_COLUMN_NAME = 2
    BY_INDEX = 3


@dataclass
class DeviceSelector:
    def __init__(self, *args):
        """
        Handles three cases:
        1. Single argument: with value 'all'
        2. Two arguments: a column name and list of values
        3. Single argument: list of indices
        """
        if len(args) == 1 and isinstance(args[0], str) and args[0] == "all":
            self.selector = SelectCriteria.ALL

        elif len(args) == 2 and isinstance(args[0], str) and isinstance(args[1], list):
            self.select_by = args[0]
            self.items = args[1]
            self.selector = SelectCriteria.BY_COLUMN_NAME

        elif len(args) == 1 and isinstance(args[0], list):
            self.select_by = "index"
            self.items = args[0]
            self.selector = SelectCriteria.BY_INDEX

        else:
            raise ValueError(
                "Invalid arguments. Valid forms are: (1) 'all', (2) str+list, or (3) list"
            )


@dataclass
class ExcludedDevices:

    def __init__(self):
        self.excluded_devices = {}

    # connecting devices
    @validate(validate_exlusion_arg)
    def disable_lines(self, lines: DeviceSelector = DeviceSelector("all")):
        self.excluded_devices["line"] = lines

    @validate(validate_exlusion_arg)
    def disable_transformers(
        self, transformers: DeviceSelector = DeviceSelector("all")
    ):
        self.excluded_devices["trafo1ph"] = transformers

    @validate(validate_exlusion_arg)
    def disable_switches(self, switches: DeviceSelector = DeviceSelector("all")):
        self.excluded_devices["switch"] = switches

    @validate(validate_exlusion_arg)
    def disable_regulators(self, regulators: DeviceSelector = DeviceSelector("all")):
        self.excluded_devices["regulator"] = regulators

    @validate(validate_exlusion_arg)
    def disable_reclosers(self, reclosers: DeviceSelector = DeviceSelector("all")):
        self.excluded_devices["recloser"] = reclosers

    @validate(validate_exlusion_arg)
    def disable_fuses(self, fuses: DeviceSelector = DeviceSelector("all")):
        self.excluded_devices["fuse"] = fuses

    # end devices
    @validate(validate_exlusion_arg)
    def disable_asymmetric_loads(self, loads: DeviceSelector = DeviceSelector("all")):
        self.excluded_devices["asymmetric_load"] = loads

    @validate(validate_exlusion_arg)
    def disable_asymmetric_sgens(
        self, generators: DeviceSelector = DeviceSelector("all")
    ):
        self.excluded_devices["asymmetric_sgen"] = generators

    @validate(validate_exlusion_arg)
    def disable_shunts(self, shunts: DeviceSelector = DeviceSelector("all")):
        self.excluded_devices["shunt"] = shunts


class DeviceExclusionHandler(ABC):
    @abstractmethod
    def exclude_devices(self, net: pp.pandapowerNet, excluded_devices: ExcludedDevices):
        pass


class DefaultDeviceExclusionHandler(DeviceExclusionHandler):
    def disable_device(self, index, net, device_type, row):
        match device_type:
            case "line":
                mc.create_switch(
                    net,
                    bus=row["from_bus"].iloc[0],
                    phase=row["from_phase"].tolist(),
                    element=row["to_bus"].iloc[0],
                    et="b",
                    closed=True,
                )
            case "switch":
                net[device_type].at[index, "closed"] = False
            case "trafo1ph":
                bus_list = row.index.get_level_values("bus").unique().tolist()
                from_phase = row.loc[bus_list[0]]["from_phase"].tolist()
                name = row["name"].tolist()[0]
                mc.create_switch(
                    net,
                    bus=bus_list[0],
                    phase=from_phase,
                    element=bus_list[1],
                    et="b",
                    closed=True,
                    name=name,
                )

    def exclude_devices(self, net: pp.pandapowerNet, excluded_devices: ExcludedDevices):
        self.exclude_end_devices(net, excluded_devices)
        self.exclude_connecting_devices(net, excluded_devices)

    def exclude_end_devices(
        self, net: pp.pandapowerNet, excluded_devices: ExcludedDevices
    ):
        end_devices = ["asymmetric_load", "asymmetric_sgen", "shunt"]
        for key, value in excluded_devices.excluded_devices.items():
            if key in end_devices and key in net:
                if value.selector == SelectCriteria.ALL:
                    for col in ["p_mw", "q_mvar"]:
                        if col in net[key].columns:
                            net[key][col] = 0.0
                elif value.selector == SelectCriteria.BY_INDEX:
                    indices = value.items
                    for col in ["p_mw", "q_mvar"]:
                        if col in net[key].columns:
                            net[key].loc[indices, col] = 0.0
                else:
                    id_col, id_col_values = value.select_by, value.items
                    if id_col in net[key].columns:
                        mask = net[key][id_col].isin(id_col_values)
                        for col in ["p_mw", "q_mvar"]:
                            if col in net[key].columns:
                                net[key].loc[mask, col] = 0.0

    def exclude_connecting_devices(
        self, net: pp.pandapowerNet, excluded_devices: ExcludedDevices
    ):
        connecting_devices = ["line", "trafo1ph", "switch"]
        for key, value in excluded_devices.excluded_devices.items():
            if key in connecting_devices and (key in net and not net[key].empty):
                if value.selector == SelectCriteria.ALL:
                    indices = net[key].index.levels[0].to_list()
                    for index in indices:
                        row = net[key].loc[index]
                        self.disable_device(index, net, key, row)
                    net[key].drop(net[key].index, inplace=True)
                elif value.selector == SelectCriteria.BY_INDEX:
                    indices = value.items
                    for index in indices:
                        self.disable_device(index, net, key, row)
                    net[key].loc[net[key].index.isin(indices), "in_service"] = False
                else:
                    # TODO: iterate on top level multi index - done - test
                    id_col, id_col_values = value.select_by, value.items
                    if id_col in net[key].columns:
                        matching_rows = net[key][net[key][id_col].isin(id_col_values)]
                        indices = matching_rows.index.levels[0].to_list()
                        for index in indices:
                            row = net[key].loc[index]
                            self.disable_device(index, net, key, row)
                        net[key].loc[
                            net[key][id_col].isin(id_col_values), "in_service"
                        ] = False

class LoadAllocationMeasurement(ABC):
    @abstractmethod
    def run_cc_allocation(self, net: pp.pandapowerNet, measurement_data: any) -> any:
        pass

    @abstractmethod
    def run_ami_load_allocation(self, net: pp.pandapowerNet, measurement_data: any) -> any:
        pass

class Pipeline:
    def __init__(
        self,
        datasource: DataSource,
        transformer: NetworkDataTransformer = None,
        device_exclusion_handler: DeviceExclusionHandler = None,
        load_profile_controller: LoadProfileController = None,
        load_allocation_measurement: LoadAllocationMeasurement = None,
        result_handler: ResultHandler = None,
        retry_on_exception: bool = False,
    ):
        self.logger = logging.getLogger("Pipeline_Logger")
        self.datasource = datasource
        self.transformer = transformer or NoOpDataTransformer()
        self.load_profile_controller = load_profile_controller
        self.load_allocation_measurement = load_allocation_measurement
        self.device_exclusion_handler = (
            device_exclusion_handler or DefaultDeviceExclusionHandler()
        )
        self.result_handler = result_handler or ResultHandler()
        self.excluded_devices = None
        self.retry_on_exception = retry_on_exception
        self.is_retry = False
        self.net = None
        self.tr_context = {}
        self.metrics = []
        self.runid = self.create_runid()
        
    def __str__(self):
        class_info = []
        elements = ['bus', 'ext_grid', 'asymmetric_load', 'asymmetric_sgen', 'asymmetric_gen', 'line', 'switch', 'trafo1ph', 'asymmetric_shunt']    
        if self.net is not None:
            for element in elements:
                if element in self.net and len(self.net[element]) > 0:
                    class_info.append(f"{element}: {len(self.net[element])}")
        return "\n".join(class_info)
    
    def create_runid(self):
        characters = string.ascii_uppercase + string.digits
        self.runid = ''.join(random.choice(characters) for _ in range(8))
        return self.runid

    def set_excluded_devices(self, excluded_devices: ExcludedDevices):
        self.excluded_devices = excluded_devices
     
    def before_run(self, net):
        pass
  
    def before_retry(self, net):
        pass
    
    def build_net(self) -> any:
        try:
            raw_data = self.datasource.retrieve()
            self.net, self.tr_context = self.transformer.transform(raw_data)
        except Exception as e:
            self.logger.error(str(e))
            raise e
        finally:
            self.datasource = None
      

    @track_time         
    def _run_pf(self):

        extra_arg_names = ['run_control']
        extra_args = {key: self.tr_context[key] for key in extra_arg_names if key in self.tr_context}
        # extra_args['run_voltvar_control'] = False # == False -> 1.5 disabled
        # run_pf(net, tol_vmag_pu=1e-7, tol_vang_rad=1e-7, MaxIter=20, run_control=True)
        extra_args['tol_vmag_pu'] = 1e-5
        extra_args['tol_vang_rad'] = 1e-5
        extra_args['MaxIter'] = 20
        extra_args['run_control'] = True

        if len(extra_args) > 0:        
            mc.run_pf(self.net, **extra_args)
        else:
            mc.run_pf(self.net)
    
    @track_time
    def _update_step(self, load_data):
        if load_data is not None and self.load_profile_controller is not None:
            self.load_profile_controller.update_step(self.net, load_data)
    
    @track_time
    def _exclude_devices(self):
        if self.excluded_devices:
            self.device_exclusion_handler.exclude_devices(self.net, self.excluded_devices)       
    
    @track_time
    def _handle_result(self, load_data):
        if self.result_handler:
            return self.result_handler.apply(self.net, load_data)
        
    def get_metrics(self):
        return "|".join(self.metrics)
    
    def run_analysis(self, load_data:any = None) -> any:
        warnings.warn(
        "run_analysis is deprecated, use run_powerflow instead",
        DeprecationWarning,
        stacklevel=2 
        )        
        
        return self.run_powerflow(load_data)

    def run_load_allocation(self, procedure, meas_data:any = None) -> any:
        if self.load_allocation_measurement is None:
            raise "Load allocation is not setup. Create a pipeline with load_allocation_measurement"
        if procedure == 'AMI_ALLOCATION':
            self.logger.info(f"Running run_ami_load_allocation")
            return self.load_allocation_measurement.run_ami_load_allocation(self.net, meas_data)
        return self.load_allocation_measurement.run_cc_allocation(self.net, meas_data)
  

    def run_powerflow(self, load_data:any = None) -> any:
        self.metrics = []
        self.runid = self.create_runid()
        if self.net is None:
            raw_data = self.datasource.retrieve()
            self.net, self.tr_context = self.transformer.transform(raw_data)

        self._exclude_devices()
        
        self._update_step(load_data)
            
        self.before_run(self.net)
        
        try:
            self._run_pf()
        except Exception as e:
            if self.retry_on_exception and not self.is_retry:
                self.is_retry = True
                self.before_retry(self.net)
                self._run_pf()
            else:
                self.logger.error(f"Error running powerflow {str(e)}")
                raise e
        finally:
            self.datasource = None
        
        # pp.diagnostic(self.net, report_style=None)

        if self.result_handler:
            return self._handle_result(load_data)
        return self.net

