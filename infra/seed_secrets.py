#!/usr/bin/env python3
"""
infra/seed_secrets.py
~~~~~~~~~~~~~~~~~~~~~
Utility script to read the RIOT_API_KEY from the local .env file
and automatically upload it to GCP Secret Manager using the gcloud CLI.
"""

import subprocess
import sys
from pathlib import Path


def get_env_var(env_path: Path, key: str) -> str:
    """Parse .env file manually to avoid dependency on python-dotenv."""
    if not env_path.exists():
        print(f"Error: .env file not found at {env_path.resolve()}", file=sys.stderr)
        sys.exit(1)

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, v = line.split("=", 1)
            if k.strip() == key:
                return v.strip().strip("'\"")
    return ""


def main():
    root_dir = Path(__file__).resolve().parent.parent
    env_path = root_dir / ".env"

    # 1. Parse required variables from local .env
    api_key = get_env_var(env_path, "RIOT_API_KEY")
    project_id = get_env_var(env_path, "GCP_PROJECT_ID")
    environment = get_env_var(env_path, "environment") or "dev"

    if not api_key or api_key.startswith("RGAPI-your-api-key"):
        print(
            "Error: RIOT_API_KEY in .env is either empty or a placeholder.",
            file=sys.stderr,
        )
        print("Please replace it with your actual Riot API key first.", file=sys.stderr)
        sys.exit(1)

    if not project_id:
        print("Error: GCP_PROJECT_ID not defined in .env.", file=sys.stderr)
        sys.exit(1)

    secret_name = f"riot-api-key-{environment}"
    print("Syncing RIOT_API_KEY from .env to GCP Secret Manager...")
    print(f"Target Project: {project_id}")
    print(f"Target Secret:  {secret_name}")

    # 2. Call gcloud via subprocess to add the secret version
    try:
        # We pass the key via stdin to prevent exposing it in process tables
        process = subprocess.Popen(
            [
                "gcloud",
                "secrets",
                "versions",
                "add",
                secret_name,
                f"--project={project_id}",
                "--data-file=-",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        stdout, stderr = process.communicate(input=api_key)

        if process.returncode == 0:
            print("\nSuccess! Secret version created successfully in Secret Manager.")
            print(stdout.strip())
        else:
            print("\nError seeding secret to GCP Secret Manager:", file=sys.stderr)
            print(stderr.strip(), file=sys.stderr)
            sys.exit(process.returncode)

    except FileNotFoundError:
        print("\nError: 'gcloud' CLI command not found.", file=sys.stderr)
        print(
            "Please ensure Google Cloud SDK is installed and configured in your PATH.",
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
