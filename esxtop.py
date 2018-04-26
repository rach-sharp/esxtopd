import csv
import re
import maya
from collections import defaultdict
import graphitesend

g = graphitesend.init(prefix="atlas", graphite_server="192.168.1.51", group='performance', system_name="")


class StatGroup(object):
    key_string_pattern = None
    aliased_stat_names = []
    group = None

    def __init__(self, key_string, data, timestamps):
        self.match = self.key_string_pattern.match(key_string)
        self.data = data
        self.timestamps = timestamps
        self.name = None

    def export_graphite_rows(self):
        graphite_stats = []
        for original_name, alias in self.aliased_stat_names:
            for data_point, stat_time in zip(self.data[original_name], self.timestamps):
                graphite_stats.append(("{0}.{1}.{2}".format(self.name.replace(".", "_"), self.group, alias), float(data_point),
                                       maya.parse(stat_time).epoch))
        return graphite_stats


class GroupCpuStatGroup(StatGroup):
    key_string_pattern = re.compile(r"Group Cpu\(([^:]*):([^:]*)\)")

    group = "cpu"

    aliased_stat_names = [
        ("% Used", "percent_used"),
        ("% Run", "percent_run"),
        ("% System", "percent_system"),
        ("% Wait", "percent_wait"),
        ("% Ready", "percent_ready"),
        ("% Idle", "percent_idle"),
        ("Effective Min (MHz)", "effective_min_mhz")
    ]

    def __init__(self, key_string, data, timestamps):
        super().__init__(key_string, data, timestamps)
        self.id, self.name = self.match.group(1), self.match.group(2)


class GroupMemoryStatGroup(StatGroup):
    key_string_pattern = re.compile(r"Group Memory\(([^:]*):([^:]*)\)")

    group = "memory"

    aliased_stat_names = [
        ("Swapped MBytes", "swapped_mb"),
        ("Swap Read MBytes/sec", "swap_read"),
        ("Swap Written MBytes/sec", "swap_write"),
        ("Compressed Memory MBytes", "compressed_mb"),
        ("Compression MBytes/sec", "compression_write"),
        ("Decompression MBytes/sec", "compression_read"),
        ("Memory Size MBytes", "max"),
        ("Touched MBytes", "touched"),
        ("% Active Slow Estimate", "percent_active"),
        ("Shared Saved MBytes", "shared_saved"),
        ("Overhead MBytes", "overhead")
    ]

    def __init__(self, key_string, data, timestamps):
        super().__init__(key_string, data, timestamps)
        self.id, self.name = self.match.group(1), self.match.group(2)


class VirtualDiskStatGroup(StatGroup):
    key_string_pattern = re.compile(r"Virtual Disk\(([^:]*)(?::([^:]*):([^:]*))?\)")

    group = "disk"

    aliased_stat_names = [
        ("Reads/sec", "read_count"),
        ("Writes/sec", "write_count"),
        ("MBytes Read/sec", "read_mb"),
        ("MBytes Written/sec", "write_mb"),
        ("Average MilliSec/Read", "read_time"),
        ("Average MilliSec/Write", "write_time")
    ]

    def __init__(self, key_string, data, timestamps):
        super().__init__(key_string, data, timestamps)
        self.name = self.match.group(1)
        if self.match.group(2):
            self.name += "_" + self.match.group(2)
        self.controller_number = self.match.group(3)


class NetworkPortStatGroup(StatGroup):
    key_string_pattern = re.compile(r"Network Port\(([^:]*):([^:]*):([^:]*)(?::([^:]*))?\)")

    group = "network"

    aliased_stat_names = [
        ("MBits Transmitted/sec", "upload"),
        ("MBits Received/sec", "download")
    ]

    def __init__(self, key_string, data, timestamps):
        super().__init__(key_string, data, timestamps)
        self.switch_name = self.match.group(1)
        self.port_id = self.match.group(2)
        # print(key_string)
        # print(self.match.groups())
        if self.match.group(4) is None:
            self.name = self.match.group(3)
            self.vm_id = None
        else:
            self.name = self.match.group(4)
            self.vm_id = self.match.group(3)
        # print(self.name)
        # print(self.vm_id)
        # print("--------")


class NullStatGroup(StatGroup):
    key_string_pattern = re.compile(".*")

    def __init__(self, key_string, data, timestamps):
        super().__init__(key_string, data, timestamps)


class StatGroupFactory(object):
    stat_group_identifiers = {
        "Group Cpu(": GroupCpuStatGroup,
        "Group Memory(": GroupMemoryStatGroup,
        "Virtual Disk(": VirtualDiskStatGroup,
        "Network Port(": NetworkPortStatGroup
    }

    @staticmethod
    def create_stat_group(key_string: str, data: dict, timestamps):
        for identifier, stat_group in StatGroupFactory.stat_group_identifiers.items():
            if key_string.startswith(identifier):
                return stat_group(key_string, data, timestamps)
        return NullStatGroup(key_string, data, timestamps)


with open('esxtop-data.csv') as csv_file:
    reader = csv.DictReader(csv_file)
    csv_rows = [row for row in reader]
    nested_stats = defaultdict(dict)

    row_timestamps = [row["(PDH-CSV 4.0) (UTC)(0)"] for row in csv_rows]

    for field_name in reader.fieldnames:
        short_field_name = field_name.replace("\\\\localhost.home\\", "")
        field_name_parts = short_field_name.split("\\")
        if len(field_name_parts) >= 2:
            nested_stats[field_name_parts[0]][field_name_parts[1]] = [row[field_name] for row in csv_rows]

    stat_groups = []
    vm_names = []
    for stat_group_key, stat_group_data in nested_stats.items():
        result = StatGroupFactory.create_stat_group(stat_group_key, stat_group_data, row_timestamps)
        if isinstance(result, NetworkPortStatGroup) and result.vm_id is not None:
            vm_names.append(result.name)
        stat_groups.append(result)

    graphite_rows = []
    for s in stat_groups:
        if s.name in vm_names:
            rows = s.export_graphite_rows()
            if "styx.network" in rows[0][0]:
                continue
            graphite_rows.extend(rows)
    print(graphite_rows)
    g.send_list(graphite_rows)
