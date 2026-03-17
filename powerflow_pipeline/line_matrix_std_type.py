import math


class MultiConductorLineMatrixStdType:
    def __init__(self, material_code, phase_conductor_values, neutral_conductor_values):
        self.material_code = material_code
        self.phase_conductor_values = phase_conductor_values
        self.neutral_conductor_values = neutral_conductor_values
        self.matrix_linetypes = {}
        self.init_matrix_linetypes()

    def init_matrix_linetypes(self):


        # connected phase = A or B or C
        self.matrix_linetypes[f"{self.material_code}_1"] = {}
        self.matrix_linetypes[f"{self.material_code}_1"]["max_i_ka"] = [
            self.phase_conductor_values["LINE_MAX_I_KA"]
        ]

        for v in ["r", "x", "b", "g"]:
            unit = 'OHM' if v in ['r', 'x'] else 'US'
            unit_lc = unit.lower()
            self.matrix_linetypes[f"{self.material_code}_1"][f"{v}_1_{unit_lc}_per_km"] = [
                self.phase_conductor_values[f"LINE_{v.upper()}_{unit}_PER_KM"]
            ]
            self.matrix_linetypes[f"{self.material_code}_1"][f"{v}_2_{unit_lc}_per_km"] = None # []
            self.matrix_linetypes[f"{self.material_code}_1"][f"{v}_3_{unit_lc}_per_km"] = None # []
            self.matrix_linetypes[f"{self.material_code}_1"][f"{v}_4_{unit_lc}_per_km"] = None # []

        # connectedphase = AB or BC or CA
        self.matrix_linetypes[f"{self.material_code}_2"] = {}
        self.matrix_linetypes[f"{self.material_code}_2"]["max_i_ka"] = [
            self.phase_conductor_values["LINE_MAX_I_KA"]
        ] * 2

        for v in ["r", "x", "b", "g"]:
            unit = 'OHM' if v in ['r', 'x'] else 'US'
            unit_lc = unit.lower()
            self.matrix_linetypes[f"{self.material_code}_2"][f"{v}_1_{unit_lc}_per_km"] = [
                self.phase_conductor_values[f"LINE_{v.upper()}_{unit}_PER_KM"],
                0,
            ]
            self.matrix_linetypes[f"{self.material_code}_2"][f"{v}_2_{unit_lc}_per_km"] = [
                0,
                self.phase_conductor_values[f"LINE_{v.upper()}_{unit}_PER_KM"],
            ]
            self.matrix_linetypes[f"{self.material_code}_2"][f"{v}_3_{unit_lc}_per_km"] = None # []
            self.matrix_linetypes[f"{self.material_code}_2"][f"{v}_4_{unit_lc}_per_km"] = None # []

        # connectedphase = AN or BN or CN
        if not any(math.isnan(value) for value in self.neutral_conductor_values.values()):
            self.matrix_linetypes[f"{self.material_code}_2_N"] = {}
            self.matrix_linetypes[f"{self.material_code}_2_N"]["max_i_ka"] = [
                self.phase_conductor_values["LINE_MAX_I_KA"],
                self.phase_conductor_values["LINE_MAX_I_KA"],
            ]

            for v in ["r", "x", "b", "g"]:
                unit = 'OHM' if v in ['r', 'x'] else 'US'
                unit_lc = unit.lower()
                self.matrix_linetypes[f"{self.material_code}_2_N"][f"{v}_1_{unit_lc}_per_km"] = [
                    self.phase_conductor_values[f"LINE_{v.upper()}_{unit}_PER_KM"],
                    0,
                ]
                self.matrix_linetypes[f"{self.material_code}_2_N"][f"{v}_2_{unit_lc}_per_km"] = [
                    0,
                    self.phase_conductor_values[f"LINE_{v.upper()}_{unit}_PER_KM"],
                ]
                self.matrix_linetypes[f"{self.material_code}_2_N"][f"{v}_3_{unit_lc}_per_km"] = None # []
                self.matrix_linetypes[f"{self.material_code}_2_N"][f"{v}_4_{unit_lc}_per_km"] = None # []

        # connectedphase = ABC
        self.matrix_linetypes[f"{self.material_code}_3"] = {}
        self.matrix_linetypes[f"{self.material_code}_3"]["max_i_ka"] = [
            self.phase_conductor_values["LINE_MAX_I_KA"]
        ] * 3

        for v in ["r", "x", "b", "g"]:
            unit = 'OHM' if v in ['r', 'x'] else 'US'
            unit_lc = unit.lower()
            self.matrix_linetypes[f"{self.material_code}_3"][f"{v}_1_{unit_lc}_per_km"] = [
                self.phase_conductor_values[f"LINE_{v.upper()}_{unit}_PER_KM"],
                0,
                0,
            ]
            self.matrix_linetypes[f"{self.material_code}_3"][f"{v}_2_{unit_lc}_per_km"] = [
                0,
                self.phase_conductor_values[f"LINE_{v.upper()}_{unit}_PER_KM"],
                0,
            ]
            self.matrix_linetypes[f"{self.material_code}_3"][f"{v}_3_{unit_lc}_per_km"] = [
                0,
                0,
                self.phase_conductor_values[f"LINE_{v.upper()}_{unit}_PER_KM"],
            ]
            self.matrix_linetypes[f"{self.material_code}_3"][f"{v}_4_{unit_lc}_per_km"] = None # []

        # connectedphase = ABN or BCN or CAN
        if not any(math.isnan(value) for value in self.neutral_conductor_values.values()):
            self.matrix_linetypes[f"{self.material_code}_3_N"] = {}
            self.matrix_linetypes[f"{self.material_code}_3_N"]["max_i_ka"] = [
                self.phase_conductor_values["LINE_MAX_I_KA"],
                self.phase_conductor_values["LINE_MAX_I_KA"],
                self.phase_conductor_values["LINE_MAX_I_KA"],
            ]

            for v in ["r", "x", "b", "g"]:
                unit = 'OHM' if v in ['r', 'x'] else 'US'
                unit_lc = unit.lower()
                self.matrix_linetypes[f"{self.material_code}_3_N"][f"{v}_1_{unit_lc}_per_km"] = [
                    self.phase_conductor_values[f"LINE_{v.upper()}_{unit}_PER_KM"],
                    0,
                    0,
                ]
                self.matrix_linetypes[f"{self.material_code}_3_N"][f"{v}_2_{unit_lc}_per_km"] = [
                    0,
                    self.phase_conductor_values[f"LINE_{v.upper()}_{unit}_PER_KM"],
                    0,
                ]
                self.matrix_linetypes[f"{self.material_code}_3_N"][f"{v}_3_{unit_lc}_per_km"] = [
                    0,
                    0,
                    self.phase_conductor_values[f"LINE_{v.upper()}_{unit}_PER_KM"],
                ]
                self.matrix_linetypes[f"{self.material_code}_3_N"][f"{v}_4_{unit_lc}_per_km"] = None # []

        # connectedphase = ABCN
        # connectedphase = ABCN
        if not any(math.isnan(value) for value in self.neutral_conductor_values.values()):
            self.matrix_linetypes[f"{self.material_code}_4"] = {}
            self.matrix_linetypes[f"{self.material_code}_4"]["max_i_ka"] = [
                self.phase_conductor_values["LINE_MAX_I_KA"],
                self.phase_conductor_values["LINE_MAX_I_KA"],
                self.phase_conductor_values["LINE_MAX_I_KA"],
                self.phase_conductor_values["LINE_MAX_I_KA"],
            ]

            for v in ["r", "x", "b", "g"]:
                unit = 'OHM' if v in ['r', 'x'] else 'US'
                unit_lc = unit.lower()
                self.matrix_linetypes[f"{self.material_code}_4"][f"{v}_1_{unit_lc}_per_km"] = [
                    self.phase_conductor_values[f"LINE_{v.upper()}_{unit}_PER_KM"],
                    0,
                    0,
                    0,
                ]
                self.matrix_linetypes[f"{self.material_code}_4"][f"{v}_2_{unit_lc}_per_km"] = [
                    0,
                    self.phase_conductor_values[f"LINE_{v.upper()}_{unit}_PER_KM"],
                    0,
                    0,
                ]
                self.matrix_linetypes[f"{self.material_code}_4"][f"{v}_3_{unit_lc}_per_km"] = [
                    0,
                    0,
                    self.phase_conductor_values[f"LINE_{v.upper()}_{unit}_PER_KM"],
                    0,
                ]
                self.matrix_linetypes[f"{self.material_code}_4"][f"{v}_4_{unit_lc}_per_km"] = [
                    0,
                    0,
                    0,
                    self.phase_conductor_values[f"LINE_{v.upper()}_{unit}_PER_KM"]
                ]
                
    def get_matrix_linetypes(self):
        return self.matrix_linetypes
