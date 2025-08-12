"""Utility modules for Azure, GitHub, and Fabric clients.

Avoid importing heavy/optional dependencies at package import time to keep
runtime environments (e.g., CI/CD workflows) lightweight. Import LLM-related
utilities directly from their modules when needed, e.g.:

    from src.stacktrend.utils.llm_classifier import LLMRepositoryClassifier
"""

from .deploy_notebooks import FabricNotebookDeployer
from .create_service_principal import main as create_service_principal

__all__ = [
    "FabricNotebookDeployer",
    "create_service_principal",
]