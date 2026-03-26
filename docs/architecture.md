[//]: # (# Architecture Overview)

[//]: # ()

[//]: # (This project is a **Python-first, YAML-driven data orchestration framework**)

[//]: # (built on **Dagster**. It provides **dbt-like materialization semantics**)

[//]: # (without using dbt, Jinja, or SQL macros.)

[//]: # ()

[//]: # (The framework is designed as a **general-purpose library** for building)

[//]: # (reliable, schema-aware data pipelines.)

[//]: # ()

[//]: # (---)

[//]: # ()

[//]: # (## Core Design Principles)

[//]: # ()

[//]: # (- Explicit schema contracts)

[//]: # (- Deterministic execution)

[//]: # (- Programmatic asset generation)

[//]: # (- Safe and observable schema evolution)

[//]: # (- Separation of intent &#40;YAML&#41; from execution &#40;Python&#41;)

[//]: # (- Framework-level extensibility and maintainability)

[//]: # (- Performance-conscious materialization strategies)

[//]: # (- Testable, pure logic separate from orchestration)

[//]: # (- Clear error handling and metadata emission for Dagster UI visibility)

[//]: # (- Support for both DDL &#40;schema changes&#41; and DML &#40;data transformations&#41; in PostgreSQL)

[//]: # (- Designed for production use by multiple teams, not just a single application)

[//]: # (- Avoidance of dbt, Jinja, and SQL macros to maintain a clean separation between configuration and execution logic)

[//]: # (- Use of typed Python models &#40;e.g., Pydantic&#41; to represent schema definitions, materialization strategies, and asset)

[//]: # (  metadata, ensuring type safety and clarity throughout the codebase)

[//]: # (- Explicit validation of schema changes and materialization rules before executing any side effects, with clear error)

[//]: # (  messages to guide users in resolving issues)

[//]: # (- Support for multiple materialization strategies &#40;e.g., full refresh, incremental, append-only&#41; with clear rules and)

[//]: # (  constraints for each, ensuring that users can choose the right approach for their use case while maintaining data)

[//]: # (  integrity)

[//]: # (- Though not using dbt, the framework provides similar capabilities for defining transformations and dependencies)

[//]: # (  between assets, allowing users to build complex data pipelines with clear lineage and modularity)

[//]: # ()

[//]: # (---)

[//]: # ()

[//]: # (## High-Level Flow)

[//]: # ()

[//]: # (```text)

[//]: # (YAML Configuration)

[//]: # (        ↓)

[//]: # (Parsed into Typed Python Models)

[//]: # (        ↓)

[//]: # (Build Dagster Assets, jobs, Sensors, AssertCheks)

[//]: # (```  )

[//]: # ()
