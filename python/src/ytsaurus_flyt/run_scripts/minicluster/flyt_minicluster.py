"""Runs a PyFlink script whose local MiniCluster gets flyt's ``config.yaml``.

PyFlink's local execution environment ignores ``$FLINK_CONF_DIR/config.yaml``, so this installs a
context environment with that configuration before the script runs, the way ``flink run`` and the
application-mode ``PythonDriver`` do. Options the script sets in code still win, as in application mode.

Usage: ``python flyt_minicluster.py <conf_dir> <script.py> [args...]``.
"""

import runpy
import sys


def install_context_environment(conf_dir):
    from pyflink.java_gateway import get_gateway

    jvm = get_gateway().jvm
    conf = jvm.org.apache.flink.configuration.GlobalConfiguration.loadConfiguration(conf_dir)
    # A local (MiniCluster) executor that waits for the job: a detached one would end with the script.
    conf.setString("execution.target", "local")
    conf.setString("execution.attached", "true")
    jvm.org.apache.flink.client.program.StreamContextEnvironment.setAsContext(
        jvm.org.apache.flink.core.execution.DefaultExecutorServiceLoader(),
        conf,
        jvm.java.lang.Thread.currentThread().getContextClassLoader(),
        False,  # enforceSingleJobExecution: a script may run several jobs
        False,  # suppressSysout
    )


def main(argv):
    if len(argv) < 2:
        raise SystemExit("usage: flyt_minicluster.py <conf_dir> <script.py> [args...]")
    conf_dir, script = argv[0], argv[1]
    install_context_environment(conf_dir)
    sys.argv = [script] + list(argv[2:])
    runpy.run_path(script, run_name="__main__")


if __name__ == "__main__":
    main(sys.argv[1:])
