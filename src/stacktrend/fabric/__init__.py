"""Microsoft Fabric integration utilities and configurations."""

from .data_factory_pipelines import FabricDataFactoryPipelines, generate_pipeline_json_files

__all__ = ["FabricDataFactoryPipelines", "generate_pipeline_json_files"]