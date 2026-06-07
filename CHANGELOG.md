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
