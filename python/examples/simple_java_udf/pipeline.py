import logging
import sys

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

logging.basicConfig(stream=sys.stderr, level=logging.INFO)
log = logging.getLogger(__name__)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    t_env.get_config().set("rest.port", "27050")
    t_env.get_config().set("rest.bind-address", "0.0.0.0")

    t_env.execute_sql("""
        CREATE TABLE words_source (word STRING) WITH (
            'connector' = 'datagen',
            'rows-per-second' = '5',
            'fields.word.kind' = 'random',
            'fields.word.length' = '6'
        )
    """)

    t_env.create_java_temporary_function("simple_hash", "org.example.SimpleHashUdf")

    t_env.execute_sql("""
        CREATE TABLE hash_sink (word STRING, h INT) WITH ('connector' = 'print')
    """)

    log.info("datagen, Java UDF, print sink")
    t_env.execute_sql("""
        INSERT INTO hash_sink
        SELECT word, simple_hash(word) AS h FROM words_source
    """).wait()


if __name__ == "__main__":
    main()
