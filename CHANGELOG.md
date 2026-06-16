## [1.2.1](https://github.com/reliquo/surreal-dbops/compare/v1.2.0...v1.2.1) (2026-06-16)


### Other Changes

* apply rustfmt formatting in controller test ([691f6c8](https://github.com/reliquo/surreal-dbops/commit/691f6c84fe305e1216681239aa365fadce18251f))
* ensure namespace user creation queries properly switch context and check for execution errors ([654026c](https://github.com/reliquo/surreal-dbops/commit/654026c22a038dfde53405a3bc4a7449f44d3c56))
* treat existing surreal namespace as success ([182acd9](https://github.com/reliquo/surreal-dbops/commit/182acd97c774bd3157f918b60aae612cbd30a23c))

# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased] (Next)

### Added
- **Custom Resource Definitions (CRDs)**: Implement `Instance`, `Namespace`, `Database`, `Schema`, and `Rollout` CRDs to declare and manage logical database state.
- **Reconciliation Controllers**:
  - `InstanceController`: Validates connectivity using SurrealDB client and credentials from raw values or secret references.
  - `NamespaceController`: Programmatically creates namespaces in SurrealDB cluster using `DEFINE NS`.
  - `DatabaseController`: Programmatically creates databases within namespaces using `DEFINE DB`.
  - `SchemaController`: Templates schemas, resolves Secret/ConfigMap-backed variables, and triggers `Rollout` resources.
  - `RolloutController`: Inspects SurrealDB database catalogs, computes SurrealQL statement diffs (using `INFO FOR DB` and `INFO FOR TABLE`), enforces approval policies on destructive changes, and batches migrations.
- **Mutating Admission Webhook**: Adds Axum HTTPS webhook server to intercept approvals and automatically patch `Rollout` resources with audit logs (`approved-by` and `approved-at` annotations).
- **Helm Chart**: Fully configurable Helm chart (`charts/surreal-dbops`) with cert-manager integration for webhook TLS generation and RBAC resources.
- **Local Testing & KIND E2E**:
  - Unit tests for catalog diff parsing and credential/template resolution.
  - Interactive integration test suite executing full lifecycle flow against a Kubernetes API server.
  - Shell-based KIND E2E harness (`scripts/test-e2e.sh`) automating cert-manager, local SurrealDB, and operator deployment test scenarios.
- **GitHub Actions Pipelines**: CI/CD automation for formatting check, Clippy lints, Cargo test verification, Helm lint, and Docker/Helm OCI registry publishing.
