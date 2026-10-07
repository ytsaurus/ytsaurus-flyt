-- Minimal Flink SQL script for `flyt run`: datagen -> aggregate -> print sink.
-- Leading SETs configure the TableEnvironment; this one keeps the job bounded.
SET 'execution.runtime-mode' = 'batch';
SET 'parallelism.default' = '1';

CREATE TABLE words_source (
    word STRING
) WITH (
    'connector' = 'datagen',
    'number-of-rows' = '1000',
    'fields.word.kind' = 'random',
    'fields.word.length' = '2'
);

CREATE TABLE word_counts_sink (
    word STRING,
    cnt BIGINT
) WITH (
    'connector' = 'print'
);

-- A query: rows are collected on the driver and written to --result-table
-- (or echoed to stderr when no table is given).
SELECT word, COUNT(*) AS cnt FROM words_source GROUP BY word ORDER BY cnt DESC LIMIT 10;

-- INSERTs are batched into one statement set and submitted as a single job.
INSERT INTO word_counts_sink SELECT word, COUNT(*) FROM words_source GROUP BY word;
