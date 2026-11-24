"""Renders ``config.yaml`` of a YT job again with the Flink options of the run over the profile's.

Usage: ``python flyt_flink_conf.zip <config.yaml> <env_var> <cluster_mode> <python_bin> <profile_options>``: this
file is the archive's ``__main__``, next to flyt's configuration modules. The run's options are a JSON object in
the environment variable, the profile's a JSON object of flat keys.
"""

import json
import os
import sys

from ytsaurus_flyt.application import application_cluster
from ytsaurus_flyt.config import FlytConfig
from ytsaurus_flyt.flink_conf import (
    FlinkConfigError,
    flatten_flink_config,
    flink_config_warnings,
    render_flink_config_yaml,
)
from ytsaurus_flyt.minicluster import minicluster


def main(argv):
    conf_path, env_var, cluster_mode, python_bin, profile_options = argv
    options = {**json.loads(profile_options), **flatten_flink_config(json.loads(os.environ[env_var]))}
    config = FlytConfig(cluster_mode=cluster_mode, python_bin=python_bin)
    for warning in flink_config_warnings(config, options):
        print(f"Flink configuration: {warning}", file=sys.stderr)
    try:
        cluster = (application_cluster if config.is_application_cluster else minicluster)(config, options)
    except FlinkConfigError as e:
        sys.exit(f"Flink configuration: {e}")
    with open(conf_path, "w", encoding="utf-8") as f:
        f.write(render_flink_config_yaml(cluster.flink_config))


if __name__ == "__main__":
    main(sys.argv[1:])
