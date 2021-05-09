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


@dataclass
class LauncherConfig:
    _target_: str = (
        "hydra_plugins.script_launcher.script_launcher.ScriptLauncher"
    )
    batch_script_template: str = "???"


ConfigStore.instance().store(
    group="hydra/launcher", name="script", node=LauncherConfig
)


class ScriptLauncher(Launcher):
    def __init__(self, batch_script_template: str, **kwargs) -> None:
        self.config: Optional[DictConfig] = None
        self.config_loader: Optional[ConfigLoader] = None
        self.task_function: Optional[TaskFunction] = None

        self.batch_script_template = batch_script_template

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

        # Make sure cluster output/error folder exists

        # Add task array flag to header

        # Construct args list
        args_list = "ARGS=(\n"
        for overrides in job_overrides:
            args_list += " ".join(filter_overrides(overrides)) + "\n"
        args_list += ")"

        # Construct batch script
        batch_script = self.batch_script_template
        batch_script = batch_script.replace("<PUT_ARGS>", args_list)
        batch_script = batch_script.replace("<PUT_NUM_ARGS>", str(len(job_overrides)))

        # Write batch script
        num_previous_batch_scripts = len(list(sweep_dir.glob("batch_script_*.sh")))
        batch_script_path = sweep_dir / f"batch_script_{num_previous_batch_scripts}.sh"
        batch_script_path.write_text(batch_script)

        # Submit the batch script
        log.info(f"ScriptLauncher is submitting {len(job_overrides)} jobs")
        try:
            subprocess.run(["sbatch", batch_script_path])
        except FileNotFoundError:
            raise EnvironmentError("The sbatch command could not be found.")