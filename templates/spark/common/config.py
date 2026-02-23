# Configuration Management for Fabric Spark Notebooks
# Centralizes connection strings, paths, and environment-specific settings.

import json
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger("FabricConfig")


class FabricConfig:
    """
    Configuration manager for Fabric ETL notebooks.
    Loads settings from a JSON config file or notebook parameters.
    """

    def __init__(self, config_path: str = None, environment: str = "dev"):
        self.environment = environment
        self._config = {}

        if config_path:
            self._load_from_file(config_path)
        else:
            self._load_defaults()

    def _load_from_file(self, config_path: str) -> None:
        """Load configuration from a JSON file in the Lakehouse."""
        try:
            with open(config_path, "r") as f:
                all_config = json.load(f)
            self._config = all_config.get(self.environment, all_config.get("default", {}))
            logger.info(f"Loaded config for environment: {self.environment}")
        except Exception as e:
            logger.warning(f"Failed to load config from {config_path}: {e}. Using defaults.")
            self._load_defaults()

    def _load_defaults(self) -> None:
        """Load default configuration values."""
        self._config = {
            "source": {
                "type": "postgresql",
                "host": "{{server}}.postgres.database.azure.com",
                "port": 5432,
                "database": "{{database}}",
                "schema": "public",
                "ssl_mode": "require",
            },
            "target": {
                "type": "lakehouse",
                "lakehouse_name": "lh_{{project}}",
                "warehouse_name": "wh_{{project}}",
            },
            "key_vault": {
                "name": "kv-{{project}}-{{env}}",
                "secret_prefix": "db-",
            },
            "etl": {
                "batch_size": 10000,
                "max_retries": 3,
                "retry_delay_seconds": 30,
                "log_level": "INFO",
            },
        }

    @property
    def source_jdbc_url(self) -> str:
        """Get the JDBC URL for the source database."""
        src = self._config.get("source", {})
        host = src.get("host", "localhost")
        port = src.get("port", 5432)
        database = src.get("database", "")
        return f"jdbc:postgresql://{host}:{port}/{database}"

    @property
    def source_schema(self) -> str:
        return self._config.get("source", {}).get("schema", "public")

    @property
    def lakehouse_name(self) -> str:
        return self._config.get("target", {}).get("lakehouse_name", "")

    @property
    def warehouse_name(self) -> str:
        return self._config.get("target", {}).get("warehouse_name", "")

    def get_jdbc_properties(self, key_vault_name: str = None, secret_name: str = None) -> Dict[str, str]:
        """
        Build JDBC connection properties.
        Uses Key Vault for credentials in Fabric.
        """
        kv = self._config.get("key_vault", {})
        kv_name = key_vault_name or kv.get("name", "")
        secret = secret_name or f"{kv.get('secret_prefix', '')}password"

        try:
            password = mssparkutils.credentials.getSecret(kv_name, secret)
        except Exception:
            logger.warning("Could not retrieve secret from Key Vault. Using placeholder.")
            password = "{{password_placeholder}}"

        return {
            "user": "{{username}}",
            "password": password,
            "driver": "org.postgresql.Driver",
            "ssl": "true",
            "sslmode": self._config.get("source", {}).get("ssl_mode", "require"),
        }

    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value by dot-notation key."""
        keys = key.split(".")
        value = self._config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default
        return value if value is not None else default


# ============================================================
# Convenience function for notebooks
# ============================================================

def load_config(environment: str = "dev", config_path: str = None) -> FabricConfig:
    """Load configuration for the current environment."""
    return FabricConfig(config_path=config_path, environment=environment)
