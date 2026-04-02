"""Simple PyFlink wordcount example for YTsaurus Vanilla operations.

This pipeline reads strings from a datagen source, splits them into words,
counts occurrences, and prints the results. It demonstrates the basic
PyFlink setup that runs inside a YT Vanilla operation.

Usage:
    # Locally (for testing):
    python pipeline.py

    # On YTsaurus (via ytsaurus-flyt), from repo python/:
    # flyt run "examples/simple_wordcount/pipeline.py"
"""

import logging
import sys

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # So kubectl port-forward pod/... 27050:27050 can reach the REST handler (defaults bind elsewhere).
    # Example only: binding to 0.0.0.0 exposes the REST port on all interfaces inside the job network;
    # restrict in production if your threat model requires it.
    t_env.get_config().set("rest.port", "27050")
    t_env.get_config().set("rest.bind-address", "0.0.0.0")

    # Create a datagen source (generates random words)
    t_env.execute_sql("""
        CREATE TABLE words_source (
            word STRING
        ) WITH (
            'connector' = 'datagen',
            'rows-per-second' = '10',
            'fields.word.kind' = 'random',
            'fields.word.length' = '5'
        )
    """)

    # Create a print sink
    t_env.execute_sql("""
        CREATE TABLE word_counts_sink (
            word STRING,
            cnt BIGINT
        ) WITH (
            'connector' = 'print'
        )
    """)

    # Run the wordcount query
    logger.info("Starting wordcount pipeline...")
    result = t_env.execute_sql("""
        INSERT INTO word_counts_sink
        SELECT word, COUNT(*) AS cnt
        FROM words_source
        GROUP BY word
    """)

    # In application mode, we must wait for the job to finish
    result.wait()
    logger.info("Wordcount pipeline finished.")


if __name__ == "__main__":
    main()
