# Roadmap

This roadmap tracks planned work and open decisions. See
[DESIGN_PLAN.md](./DESIGN_PLAN.md) for the architecture,
[README.md](./README.md) for current capabilities, and
[CHANGELOG.md](./CHANGELOG.md) for release history and known limitations.

The roadmap records detailed tasks and questions so they aren't overlooked.
Items are a reminder of work to consider, not a commitment to implement them
exactly as written. Revisit priorities, decisions, and milestones as the
project evolves.

## 0.2.0: Configuration contract and user experience

- YAML behavior contract
  - [x] Inventory every supported YAML field, default, and accepted value.
  - [x] Add configuration-behavior tests for existing REST connector YAML.
  - [x] Add configuration-behavior tests for existing Parquet connector YAML.
  - [x] Add configuration-behavior tests for existing DuckDB and Parquet
    target YAML.
  - [x] Add configuration-behavior tests for existing merge and cursor YAML.
  - [x] Document YAML behavior only after its corresponding tests establish
    the supported contract.

- YAML configuration reference
  - [x] Document existing connector YAML fields, defaults, accepted values, and
    validation rules.
  - [x] Document target, merge, and cursor YAML fields, defaults, accepted
    values, and validation rules.

- First project experience
  - [x] Define and document a recommended project structure.
  - [x] Document how multiple pipeline configurations should be organized.
  - [ ] Add a complete example project containing source, target, merge, and
        cursor configuration.
  - [ ] Add a guide: "Create your first project".
  - [ ] Add a guide: "Growing from one pipeline to multiple pipelines".

- Task-oriented guides
  - [ ] Add a configuration example for a first successful Parquet load.
  - [ ] Add an append merge configuration example.
  - [ ] Add an upsert merge configuration example.
  - [x] Add an SCD2 merge configuration example with static Parquet snapshots.

- Actionable user experience
  - [ ] Document common YAML validation failures, their causes, and their
    resolutions.
  - [ ] Document how YAML configuration maps to the equivalent Python API.
  - [ ] Add a concise onboarding path that links a first successful load to
    reference and troubleshooting material.

- Behavior-first configuration workflow
  - [ ] Document in `CONTRIBUTING.md` that supported YAML behavior requires
    configuration-behavior tests before user-facing documentation is updated.
  - [ ] Document the same configuration-contract workflow in the README and
    configuration-reference authoring guidance.
  - [ ] Add a CI check that validates published YAML examples against the
    supported configuration contract.

- Compatibility communication
  - [ ] Explain to users how the existing `0.x` compatibility policy in
    `RELEASING.md` applies to documented YAML behavior and Python APIs,
    including breaking-change and migration guidance.

- [ ] Release 0.2.0 with the existing configuration contract and user
  documentation.

## 0.3.0: Production-pilot capabilities and validation

Define the pilot and its safety constraints before building the capabilities
below. Open decisions are not settled by this checklist: document the chosen
scope, assumptions, and trade-offs, then implement and test what the pilot
requires. Keep the Arrow boundary and the existing orchestration boundary.

- Pilot scope and acceptance
  - [ ] Choose representative sources, destinations, merge strategies, data
    volumes, and operational environment for the pilot.
  - [ ] Define acceptance thresholds and a reconciliation method that can
    detect missing, duplicate, and incorrect records.
  - [ ] Decide whether the pilot must handle schema changes, deletions,
    late-arriving records, and multiple versions per key; document exclusions
    and require representative tests for included behavior.
  - [ ] Confirm which planned CSV and OAuth capabilities are needed for the
    selected pilot and record their priority against its other prerequisites.

- Incremental correctness
  - [ ] Define and document the explicit cursor contract: incremental loading
    is opt-in; cursor behavior is selected or governed by documented defaults,
    not inferred. Clearly reject unsupported combinations and distinguish
    validated configuration from source guarantees the user must establish.
  - [ ] Define the source assumptions for destination-derived high
    watermarks, including ordering, uniqueness or ties, inclusive versus
    exclusive boundaries, late arrivals, and source changes during offset
    pagination.
  - [ ] Decide and document the pilot's behavior when those assumptions do not
    hold, including any unsupported cases, safeguards, or recovery process.
  - [ ] Verify and document how each pilot merge strategy affects the
    persisted watermark, including SCD2's unchanged rows and possible replay.
  - [ ] Test incremental loads against the chosen boundary, replay,
    late-arrival, pagination, and reconciliation expectations.

- Write isolation and recovery
  - [ ] Decide whether concurrent writers to one destination are unsupported,
    serialized externally, or controlled by the library; document and test the
    chosen pilot policy for each relevant target.
  - [ ] Define retry and recovery expectations for each pilot merge strategy,
    including failures during a load and retries after an uncertain outcome.
  - [ ] Test the chosen failure, retry, and concurrent-write scenarios; do not
    treat atomic file replacement or an individual transaction as a complete
    concurrency guarantee.

- Pilot workload and observability contract
  - [ ] Set pilot limits and targets for data size, page size, throughput,
    duration, memory, and temporary disk usage.
  - [ ] Measure representative pilot paths against those targets, including
    REST response decoding, Parquet rewrites, and SCD2 materialization where
    applicable; document constraints and any required remediation.
  - [ ] Define pilot load-result, log, metric, and failure-field semantics,
    including what row counts mean for each merge strategy and how unchanged
    SCD2 rows are reported. Decide which fields are available and which are
    deferred.
  - [ ] Define and test secret redaction for diagnostic output, including
    OAuth credentials and tokens.

- Typed CSV ingestion
  - [ ] Define YAML configuration for a local CSV connector with an explicit
    declared Arrow schema.
  - [ ] Implement CSV ingestion using the declared Arrow schema.
  - [ ] Add behavior tests for typed CSV parsing, null handling, and invalid
    values.
  - [ ] Document typed CSV ingestion, declared schemas, and parsing failures.
  - [ ] Add a configuration example for a typed CSV load.

- OAuth 2.0 client-credentials authentication
  - [ ] Define YAML configuration for OAuth 2.0 client-credentials
    authentication using environment-backed secrets.
  - [ ] Implement client-credentials token acquisition and reuse for REST
    requests.
  - [ ] Add failure-path tests for OAuth 2.0 client-credentials authentication.
  - [ ] Document OAuth 2.0 client-credentials configuration and safe secret
    handling.
  - [ ] Add a configuration example for a REST load with OAuth 2.0
    client-credentials authentication.

- Operational visibility without orchestration
  - [ ] Emit structured logs for troubleshooting.
  - [ ] Emit metrics for load monitoring.
  - [ ] Emit load statistics for troubleshooting.
  - [ ] Document observability output and integration patterns for logs,
    metrics, and load statistics.
  - Scheduling, orchestration, lineage, auditing, and governance remain the
    responsibility of dedicated external systems and destination metadata.

- Production-pilot validation
  - [ ] Confirm the pilot prerequisites, constraints, and acceptance criteria
    above are met before loading production data.
  - [ ] Run a controlled production pilot against representative workloads.
  - [ ] Reconcile the results and exercise the agreed failure and recovery
    scenarios against the acceptance criteria.
  - [ ] Incorporate production-pilot findings into the release criteria.

- [ ] Release 0.3.0 after successful production-pilot validation.

## Post-pilot reliability and stable-release readiness

This is the highest-priority roadmap after the current configuration-contract
and production-pilot roadmaps. Complete it before expanding the supported
capability surface.

- Public API and compatibility discipline
  - [ ] Define the supported public API surface and compatibility guarantees
        for the first stable release.
  - [ ] Define a deprecation policy and migration guidance for public
        configuration and Python APIs.
  - [ ] Add release-note guidance for behavior changes and compatibility
        impacts.

- Operational diagnostics
  - [ ] Ensure failures identify the relevant configuration field and
        component where practical.
  - [ ] Refine the pilot's structured load-result and failure fields into a
        stable contract for external logging and monitoring.
  - [ ] Document diagnosis for common connector, target, merge, and cursor
        failures.

- CI and release confidence
  - [ ] Run every published example configuration in CI.
  - [ ] Establish a coverage baseline and add a coverage-regression policy.
  - [ ] Test the built wheel in a clean environment with a minimal
        end-to-end ingestion smoke test.
  - [ ] Verify supported Python versions against the packaged artifact, not
        only the source checkout.

- Performance and reliability evidence
  - [ ] Expand the pilot datasets and workload measurements into representative
        benchmarks for supported connector-target paths.
  - [ ] Measure and document baseline throughput and memory behavior for those
        paths.
  - [ ] Add regression checks only where benchmarks prove stable and useful.

- Stable-release readiness
  - [ ] Define first-stable-release acceptance criteria based on pilot
        results, documentation completeness, compatibility guarantees, and
        operational evidence.
  - [ ] Release a beta only after the acceptance criteria are met.

## Post-pilot capability backlog

- [ ] Define transformation ordering and schema validation, including how
  cursor and merge fields interact with selection, renaming, casting,
  derivation, and filtering. Implement declarative transformations with Polars
  internally while preserving Arrow streams at the public boundary.
- [ ] Evaluate advanced cursor correctness: timestamp and tuple watermarks,
  tie-breakers, and lookback policies.
- [ ] Before implementing opaque cursors, define supported user-provisioned
  destination locations and configuration, checkpoint recovery, and how
  checkpoint updates coordinate with successful writes, failures, retries, and
  replay. Preserve the design plan's visibility and abstraction boundaries.
- [ ] Add additional source connectors: JSON Lines and ADBC.
- [ ] Add additional HTTP capabilities: cursor and next-link pagination, rate
  limits, API-key authentication, and Basic authentication.
- [ ] Evaluate and add destination support: ADBC targets, Iceberg and Delta,
  and Parquet dataset-directory mode.
- [ ] Document, test, and publish task-oriented guides for each capability
  before treating it as supported.
