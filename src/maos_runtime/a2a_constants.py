"""A2A provider backend 常量。"""

SIMULATOR_BACKEND = "simulator"
MULTICA_BACKEND = "multica"
MULTICA_JOB_BACKEND = "multica_job"
HERMES_BACKEND = "hermes"

MULTICA_COMPLETED_STATUSES = {"done", "in_review"}
MULTICA_FAILED_STATUSES = {"blocked", "cancelled", "canceled"}
MULTICA_JOB_COMPLETED_STATUSES = {"completed"}
MULTICA_JOB_FAILED_STATUSES = {"failed", "cancelled", "canceled"}

PROVIDED_CONTEXT_ONLY = "provided_context_only"
LIGHTWEIGHT_RUNTIME_PROFILE = "lightweight"
LIGHTWEIGHT_MULTICA_AGENT_KEY = "general_chat"
