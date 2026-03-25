"""
Purpose of the script: loads config
"""
import logging
import yaml
import pathlib

logger = logging.getLogger(__name__)

def get_config(
    yaml_path : str="config.yaml"
) -> dict:
    """Gets the config yaml from the root directory and returns it as a dict. Can be called from any file in the project

    Parameters
    ----------
        yaml_path : str
            Path, filename, and extension of the yaml config file.
            Defaults to config.yaml

    Returns
    -------
        Dict :
            A dictionary containing details of the database, paths, etc. Should contain all the things that will
            change from one run to the next

    Example
    -------
        from shmi_improvement.utilities.helpers import get_config
        config = get_config()
    """
    with pathlib.Path(yaml_path).open('r') as file:
        return yaml.safe_load(file)

    