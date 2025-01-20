import subprocess
import psutil

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple, Any, List


@dataclass
class ExecutionCheckpoint:
    time: datetime
    cpu_count: int
    cpu_times: Any
    disk_io: Any
    network_io: Any


def _execution_stats() -> ExecutionCheckpoint:

    return ExecutionCheckpoint(
        time=datetime.now(timezone.utc),
        cpu_count=psutil.cpu_count(logical=False),
        cpu_times=psutil.cpu_times(percpu=True),
        disk_io=psutil.disk_io_counters(perdisk=False, nowrap=True),
        network_io=psutil.net_io_counters(pernic=True, nowrap=True),
    )


def print_execution_stats(start: ExecutionCheckpoint, end: ExecutionCheckpoint):
    print("**** Execution stats ****")

    cpu_sys = []
    cpu_usr = []
    cpu_idle = []
    cpu_total = []
    cpu_avg = []
    for idx, item in enumerate(start.cpu_times):
        cpu_usr.append(end.cpu_times[idx].user - item.user)
        cpu_sys.append(end.cpu_times[idx].system - item.system)
        cpu_idle.append(end.cpu_times[idx].idle - item.idle)
        cpu_total.append(cpu_usr[idx] + cpu_sys[idx] + cpu_idle[idx])
        cpu_avg.append((cpu_sys[idx] + cpu_usr[idx]) / cpu_total[idx])

    print(f'Average CPU usage: {sum(cpu_avg) / len(cpu_avg)}')
    print(f' CPU User usage per proc: {cpu_usr}')
    print(f' CPU Sys usage per proc: {cpu_sys}')
    print(f' CPU Idle usage per proc: {cpu_idle}')

    print(f'Disk Reads in bytes: '
          f'{end.disk_io.read_bytes - start.disk_io.read_bytes}')
    print(f'Disk Writes in bytes: '
          f'{end.disk_io.write_bytes - start.disk_io.write_bytes}')
    if hasattr(start.disk_io, 'busy_time'):
        print(f'Disk busy time in ms: '
              f'{end.disk_io.busy_time - start.disk_io.busy_time}')

    for nic in start.network_io:
        start_nic = start.network_io[nic]
        end_nic = end.network_io[nic]
        print(f'nic {nic} sent in bytes: '
              f'{end_nic.bytes_sent - start_nic.bytes_sent}')
        print(f'nic {nic} recv in bytes: '
              f'{end_nic.bytes_recv - start_nic.bytes_recv}')
        print(f'nic {nic} errors in: '
              f'{end_nic.errin - start_nic.errin}')
        print(f'nic {nic} errors out: '
              f'{end_nic.errout - start_nic.errout}')
        print(f'nic {nic} dropped packets coming in: '
              f'{end_nic.dropin - start_nic.dropin}')
        print(f'nic {nic} dropped packets coming out: '
              f'{end_nic.dropout - start_nic.dropout}')


def run(cmd_args: list[str], check=True, capture_output=False) \
        -> subprocess.CompletedProcess:
    """Run a subprocess"""
    print(f'{Path.cwd()}> {subprocess.list2cmdline(cmd_args)}', flush=True)

    if capture_output:
        # Subprocess doesn't have built-in support for capturing output
        # AND printing while it comes in, so we have to do it ourselves.
        # We're combining stderr with stdout, for simplicity.
        with subprocess.Popen(
            cmd_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,  # line-buffered
        ) as p:
            lines = []
            assert p.stdout is not None  # satisfy type checker
            for line in p.stdout:
                lines.append(line)
                print(line, end='', flush=True)

            p.wait()  # ensure process is 100% finished

            completed = subprocess.CompletedProcess(
                args=cmd_args,
                returncode=p.returncode,
                stdout="".join(lines),
            )
    else:
        # simpler case: just run the command
        completed = subprocess.run(cmd_args, text=True)

    if check and completed.returncode != 0:
        exit(f"FAILED running: {subprocess.list2cmdline(cmd_args)}")

    return completed


def run_with_stats(cmd_args: list[str], check=True, capture_output=False) \
        -> Tuple[subprocess.CompletedProcess, List[ExecutionCheckpoint]]:
    execution_checkpoints = [_execution_stats()]
    completed = run(cmd_args=cmd_args,
                    check=check,
                    capture_output=capture_output)
    execution_checkpoints.append(_execution_stats())
    return completed, execution_checkpoints

