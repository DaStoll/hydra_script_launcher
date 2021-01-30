import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence

from hydra.core.config_loader import ConfigLoader
from hydra.core.config_store import ConfigStore
from hydra.core.utils import (
    configure_log,
    filter_overrides,
    setup_globals,
)
from hydra.plugins.launcher import Launcher
from hydra.types import TaskFunction
from omegaconf import DictConfig

log = logging.getLogger(__name__)


_batch_script_header_default = """#!/bin/bash
#SBATCH --time ${hydra.launcher.time}
#SBATCH --cpus-per-task ${hydra.launcher.cpus_per_task}
#SBATCH --gres=gpus:${hydra.launcher.gpus}
#SBATCH --mem-per-cpu=${hydra.launcher.mem_per_cpu}
#SBATCH --job-name ${hydra.launcher.job_name}"""


@dataclass
class LauncherConfig:
    _target_: str = (
        "hydra_plugins.script_launcher.script_launcher.ScriptLauncher"
    )
    batch_script_header: str = _batch_script_header_default
    execute_command: Optional[str] = None
    pre_execute_command: Optional[str] = None
    time: str = "0-10:00"
    cpus_per_task: int = 1
    gpus: int = 0
    mem_per_cpu: int = 4000
    job_name: str = "default"
    partition: str = "???"


ConfigStore.instance().store(
    group="hydra/launcher", name="script", node=LauncherConfig
)


class ScriptLauncher(Launcher):
    def __init__(self, batch_script_header: str, execute_command: str, pre_execute_command: str, partition: str,
                 **kwargs) -> None:
        self.config: Optional[DictConfig] = None
        self.config_loader: Optional[ConfigLoader] = None
        self.task_function: Optional[TaskFunction] = None

        # Coming from the the plugin's configuration
        self.execute_command = execute_command
        self.batch_script_header = batch_script_header
        self.pre_execute_command = pre_execute_command
        self.partition = partition

    def setup(
        self,
        config: DictConfig,
        config_loader: ConfigLoader,
        task_function: TaskFunction,
    ) -> None:
        self.config = config

    def launch(self, job_overrides: Sequence[Sequence[str]], initial_job_idx: int):
        """
        :param job_overrides: a List of List<String>, where each inner list is the arguments for one job run.
        :param initial_job_idx: Initial job idx in batch.
        """
        setup_globals()
        assert self.config is not None

        configure_log(self.config.hydra.hydra_logging, self.config.hydra.verbose)
        sweep_dir = Path(str(self.config.hydra.sweep.dir))
        sweep_dir.mkdir(parents=True, exist_ok=True)
        log.info(f"Sweep output dir : {sweep_dir}")

        # Construct the switch case over different tasks
        batch_script_task_switch = "case ${SLURM_ARRAY_TASK_ID} in\n"
        for idx, overrides in enumerate(job_overrides):
            idx = initial_job_idx + idx
            overrides_string = " ".join(filter_overrides(overrides))
            if overrides_string:
                overrides_string += " "
            overrides_string += f"hydra.job.id=$SLURM_JOB_ID hydra.job.num={idx}"
            batch_script_task_switch += f"  {idx})\n    {self.execute_command} {overrides_string}\n    ;;\n"
        batch_script_task_switch += "esac\n"

        # Put together the batch script from its parts and write to sweep dir
        batch_script = "\n\n".join([self.batch_script_header, self.pre_execute_command, batch_script_task_switch])
        num_previous_batch_scripts = len(list(sweep_dir.glob("batch_script_*.sh")))
        batch_script_path = sweep_dir / f"batch_script_{num_previous_batch_scripts}.sh"
        batch_script_path.write_text(batch_script)

        # Submit the batch script
        log.info(f"ScriptLauncher is submitting {len(job_overrides)} jobs")
        try:
            subprocess.run(["sbatch", f"-p {self.partition}", batch_script_path, "--pty bash"])
        except FileNotFoundError:
            raise EnvironmentError("The sbatch command could not be found.")