"""Generic workflow-actions engine for DataHub Action Workflows.

The MFE (datahub-workflow-mfe) authors a rules document; this package validates
it (``contract``), turns lifecycle events into a context document
(``context``), evaluates triggers and conditions (``filters``), renders
templated parameters (``templating``) and runs the steps (``engine`` +
``steps``). ``action`` adapts it to the datahub-actions framework and
``source`` lets the same thing run from an ingestion recipe.
"""

from datahub_workflow_actions.contract import SCHEMA_VERSION, SOURCE_TYPE, RulesConfig, load_rules

__all__ = ["SCHEMA_VERSION", "SOURCE_TYPE", "RulesConfig", "load_rules"]
__version__ = "0.9.0"
