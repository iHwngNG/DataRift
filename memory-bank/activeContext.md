# Active Context: DataRift

## Current Focus

We have finalized Phase 1 of our infrastructure roadmap by defining GCS, BigQuery, Pub/Sub, and Secret Manager resources in our multi-environment Terraform configuration, and secured our workspace with a comprehensive `.gitignore` policy. The current focus is moving toward bootstrapping the Python 3.14 ingestion application structure under `src/ingestion/` and establishing local development virtual environments and libraries.

## Recent Changes

- Created a comprehensive [.gitignore](file:///d:/Workspace/dev/DataRift/.gitignore) file in the root workspace to prevent committing sensitive Terraform state files, secrets (`.env`, `*.tfvars` except predefined demo targets), local Python environments (`.venv/`), and compiler artifacts (IDE settings, target folders).
- Created [pubsub.tf](file:///d:/Workspace/dev/DataRift/infra/pubsub.tf) defining the `lol-ingest-trigger` topic and its subscription `lol-ingest-subscription` to decouple pipeline scheduling from active compute execution.
- Created [secret_manager.tf](file:///d:/Workspace/dev/DataRift/infra/secret_manager.tf) establishing the Secret Manager secret `riot-api-key` for secure credential storage.

## Active Decisions & Considerations

1. **Gitignore Exception Rules**:
   - The `.gitignore` is set to block all custom local `*.tfvars` but explicitly **ALLOWS** tracking our pre-defined environment configuration placeholders (`!infra/env/dev/terraform.tfvars` and `!infra/env/prod/terraform.tfvars`) so the project setup remains instantly runnable as a demo.
2. **Pub/Sub Trigger decoupling**:
   - The ingestion trigger will be kicked off by publishing events to the Pub/Sub topic, which can be easily scheduled using Cloud Scheduler or Composer.
3. **BigQuery Naming Constraints**:
   - GCP BigQuery dataset IDs do not support hyphens (`-`), only alphanumeric characters and underscores (`_`). To respect the request for `lol-lakehouse-catalog`, we set the resource ID to `lol_lakehouse_catalog` and mapped `lol-lakehouse-catalog` to the `friendly_name` attribute.
4. **Multi-Environment Module Strategy**:
   - The root files in `infra/` are structured as a reusable child module. Providers are declared within the environment directories (`infra/env/dev/` and `infra/env/prod/`), which is the recommended practice for multi-environment IaC orchestrations.
5. **Riot Ingestion Partition Rule**:
   - Ingestion code must strictly structure object path hierarchies in GCS based on the date the raw resource (e.g., match, timeline) was *created*, not when the request was executed.
6. **Project Structure Layout**:
   - The current project structure:
     ```text
     DataRift/
     ├── memory-bank/
     ├── infra/
     │   ├── env/
     │   │   ├── dev/          # Dev environment orchestrations
     │   │   └── prod/         # Prod environment orchestrations
     │   ├── bigquery.tf       # BigQuery datasets
     │   ├── gcs.tf            # Base GCS resources
     │   ├── pubsub.tf         # Pub/Sub topics & subscriptions
     │   ├── secret_manager.tf # Secret Manager definitions
     │   ├── providers.tf      # Reusable module provider requirements
     │   ├── variables.tf      # Module variables
     │   └── outputs.tf        # Module outputs
     ├── src/
     │   └── ingestion/        # Python 3.14 ingest application
     ├── dbt_project/          # dbt models for silver/gold layers
     ├── .gitignore            # Git exclusion definitions
     └── README.md
     ```

## Next Steps

1. Define the project directory structure for Python 3.14 ingestion under `src/ingestion/`.
2. Set up the local Python 3.14 virtual environment and initialize dependencies (`pyiceberg`, `requests`, `pyarrow`).
3. Develop the core Python ingestion module to fetch LoL match data and write to raw GCS.

