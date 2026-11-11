# Stack Overflow ingestion example

This example shows a small `arrow-ingest` project:

```text
stackoverflow/
├── pipelines/
│   └── questions.yaml
└── output/
```

Run the following command from the `examples/stackoverflow` directory, with
`arrow-ingest` installed in your Python environment.

## Load questions from Stack Overflow

The `questions.yaml` file reads question IDs, creation times, and links from
the public [Stack Exchange API](https://api.stackexchange.com/docs/questions).
Stack Overflow is the source of the records. You don't need an account or API
key for this example.

```console
arrow-ingest run pipelines/questions.yaml
```

The pipeline writes the `questions` table in `output/questions.duckdb`.

## Load more questions

To request more questions, wait at least one minute and run the same command:

```console
arrow-ingest run pipelines/questions.yaml
```

The pipeline uses the largest stored `creation_date` as the request's `min`
value. The API includes that value in its results, so the pipeline uses
`upsert` and `question_id` to avoid duplicate destination rows.

Run one request at a time and let it finish. The pipeline waits for any
`backoff` delay returned by the API. Delays aren't persisted or coordinated
across processes. If a run is interrupted, make sure any required API backoff
has elapsed before you start another request. For details, see the API's
[throttling guidance](https://api.stackexchange.com/docs/throttle).

This is a cursor demonstration, not a complete Stack Overflow sync. Each run
reads only one page of up to 30 questions. Questions with the same creation
time at a page boundary can prevent progress or be missed, and changes to
older questions aren't captured. See the
[cursor reference](../../docs/configuration-reference.md#load-incrementally)
and the [Stack Exchange API terms](https://stackoverflow.com/legal/api-terms-of-use).
