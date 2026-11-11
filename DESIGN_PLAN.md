# Design plan

## Vision

Describe ingestion, do not require users to implement plumbing.

The project provides a small, Arrow-native boundary between external systems
and destinations. It favors explicit configuration, predictable behavior, and
a deliberately constrained abstraction surface over a general-purpose data
platform.

## Core contracts

1. Arrow is the data contract
   - Connectors emit `pyarrow.RecordBatchReader` streams.
   - Targets consume those streams.
   - Framework-specific data structures must not cross the connector-target
     boundary.
   - Implementations should minimize conversions, copies, and full-stream
     materialization.

2. Connectors represent external systems
   - A connector represents one external system or file-based input.
   - The framework has no Source or Resource distinction.
   - Connector-specific protocol behavior, including cursor semantics, belongs
     to the connector rather than to a global framework state model.

3. Targets represent destinations
   - A target owns destination-specific loading behavior.
   - Targets expose the same Arrow-stream input contract regardless of the
     storage system.
   - Targets return operational load statistics, not framework data models.

4. Merge strategy is explicit
   - Every load declares a merge strategy.
   - The strategy set is append, replace, upsert, and SCD2.
   - Each target must reject strategies it cannot implement correctly.
   - Merge state is retained only when required by the selected strategy.
   - Merge behavior is explicit, validated, and does not add hidden metadata
     columns.

## Declarative configuration

Configuration describes how data moves from a connector, through optional
Arrow-preserving transformations, into a target. Configuration is not a
Pipeline runtime abstraction.

- Configuration is explicit, declarative, and validated before execution.
- Authentication uses credential references rather than plaintext secrets.
- Common protocol behavior belongs in reusable connector configuration.
- A common use case should not require custom Python to express it.

## Supported scope

- Connectors cover local files, HTTP APIs, and databases with ADBC support.
- Targets must provide a strong Arrow ingestion story. Parquet, DuckDB,
  Iceberg, Delta, and ADBC-compatible databases are candidate targets.
- Object storage is a location for file connectors, not a connector category.
- Message queues, CDC, spreadsheets, and SDK-only integrations are outside
  the initial scope.

## State and cursors

- Incremental loading is opt-in. Cursor behavior must be explicitly configured
  or use documented defaults, not inferred.
- Derive cursor state from retained destination data whenever possible.
- Connectors own cursor semantics; targets expose only the destination
  operations needed to resolve cursor positions and manage explicitly
  configured checkpoints.
- Resolve cursor positions before reading and advance them only after a
  successful load.
- When opaque source state cannot be derived from destination data, the
  framework may persist it in a user-provisioned, explicitly configured
  destination location. Such state must be visible to the user.
- Define failure and retry behavior for checkpoint updates so a checkpoint
  cannot advance past data that hasn't been successfully loaded.
- Cursor support must not introduce a state-management runtime abstraction or
  hidden metadata columns.

## Transformations

- Transformations consume Arrow streams and emit Arrow streams.
- Polars is the internal transformation engine; Arrow remains the public data
  contract.
- Complex business logic, analytical modeling, aggregation, and cross-dataset
  joins are outside the ingestion framework.

## Abstraction boundaries

The data-flow runtime concepts are:

- Connector
- Cursor
- Arrow stream
- Target
- Merge strategy

The framework does not expose Pipeline, Flow, Stage, Source, Resource, Asset,
Dataset, Block, Node, or registry abstractions. Configuration may describe an
ingestion workflow, but the framework does not model that workflow as an
independent runtime object. Stateless integration entry points may execute a
configuration and return operational load results without introducing a
workflow runtime model.

## Operational boundaries

- Orchestration belongs to systems such as Prefect, Dagster, Airflow, and
  Temporal, not to this framework.
- Auditing, lineage, and governance belong in destination metadata, lakehouse
  catalogs, table formats, and dedicated governance tools.
- The framework may emit logs, metrics, and load statistics for operational
  troubleshooting; these are not an auditing system.
