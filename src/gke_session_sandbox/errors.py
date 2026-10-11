"""Public exception hierarchy for the session sandbox package."""


class GkeSessionSandboxError(Exception):
    """Base class for expected library errors."""


class ConfigurationError(GkeSessionSandboxError):
    """Configuration is missing or invalid."""


class SessionConfigurationConflictError(GkeSessionSandboxError):
    """A live session was requested with configuration that differs from its record."""


class RepositoryConflictError(GkeSessionSandboxError):
    """A compare-and-swap write lost a resourceVersion race."""


class RepositoryCapacityError(GkeSessionSandboxError):
    """A session record or its pending-event queue reached a configured limit."""


class SessionLeaseHeldError(GkeSessionSandboxError):
    """Another live holder owns this session's Lease."""


class SessionLeaseLostError(GkeSessionSandboxError):
    """The caller no longer owns this session's Lease."""


class RepositoryConflictError(GkeSessionSandboxError):
    """A compare-and-swap write lost a resourceVersion race."""


class RepositoryCapacityError(GkeSessionSandboxError):
    """A session record or its pending-event queue reached a configured limit."""


class SessionLeaseHeldError(GkeSessionSandboxError):
    """Another live holder owns this session's Lease."""


class SessionLeaseLostError(GkeSessionSandboxError):
    """The caller no longer owns this session's Lease."""


class TemplateValidationError(ConfigurationError):
    """A rendered or configured sandbox template is invalid."""


class ClusterCapabilityError(GkeSessionSandboxError):
    """The target cluster lacks a required API or runtime capability."""


class ResourceOwnershipError(GkeSessionSandboxError):
    """A Kubernetes resource exists but is not owned by this session generation."""


class SandboxReadyTimeout(GkeSessionSandboxError):
    """A Sandbox did not become ready before the configured deadline."""


class SandboxBusyError(GkeSessionSandboxError):
    """An operation is unsafe while a command or file operation is active."""


class SessionExpiredError(GkeSessionSandboxError):
    """The requested session generation has passed its retention deadline."""


class StaleGenerationError(GkeSessionSandboxError):
    """A handle refers to a session generation that is no longer current."""


class WorkspaceLostError(GkeSessionSandboxError):
    """The session's dedicated workspace disappeared unexpectedly."""


class RuntimeConnectionError(GkeSessionSandboxError):
    """The runtime endpoint could not be reached or used."""


class ExecutionOutcomeUnknown(GkeSessionSandboxError):
    """A command may have run, but its final result could not be confirmed."""

    def __init__(self, command_id: str, message: str = "command outcome could not be confirmed") -> None:
        super().__init__(f"{message} (command_id={command_id})")
        self.command_id = command_id


class CleanupIncompleteError(GkeSessionSandboxError):
    """Owned Kubernetes or cloud resources remain after cleanup was requested."""


class CommandFailedError(GkeSessionSandboxError):
    """A command returned a nonzero exit code when checking was requested."""

    def __init__(self, command_id: str, exit_code: int) -> None:
        super().__init__(f"command exited with status {exit_code} (command_id={command_id})")
        self.command_id = command_id
        self.exit_code = exit_code


class CommandOutputLimitExceeded(GkeSessionSandboxError):
    """A buffered command response exceeded its configured size limit."""

    def __init__(self, command_id: str, limit_bytes: int) -> None:
        super().__init__(f"command output exceeded {limit_bytes} bytes (command_id={command_id})")
        self.command_id = command_id
        self.limit_bytes = limit_bytes


class OutputStorageLimitExceeded(GkeSessionSandboxError):
    """Persistent command output reached its configured storage limit."""

    def __init__(self, command_id: str, limit_bytes: int) -> None:
        super().__init__(f"session output storage exceeded {limit_bytes} bytes (command_id={command_id})")
        self.command_id = command_id
        self.limit_bytes = limit_bytes


class OutputWriteError(GkeSessionSandboxError):
    """Persisting command output failed before the storage quota was reached."""

    def __init__(self, command_id: str, message: str = "command output storage write failed") -> None:
        super().__init__(f"{message} (command_id={command_id})")
        self.command_id = command_id


class OutputExpiredError(GkeSessionSandboxError):
    """Persisted command output is past its retention period."""

    def __init__(self, command_id: str) -> None:
        super().__init__(f"command output has expired (command_id={command_id})")
        self.command_id = command_id


class OutputIncompleteError(GkeSessionSandboxError):
    """Only part of the command output could be recovered."""

    def __init__(self, command_id: str, message: str = "command output is incomplete") -> None:
        super().__init__(f"{message} (command_id={command_id})")
        self.command_id = command_id


class FileOperationError(GkeSessionSandboxError):
    """A sandboxd filesystem operation failed."""

    def __init__(self, message: str, *, path: str, status: int | None = None) -> None:
        super().__init__(message)
        self.path = path
        self.status = status


class FileTransferLimitExceeded(GkeSessionSandboxError):
    """A buffered or streamed file transfer exceeded its configured limit."""


class WorkspaceReadOnlyError(GkeSessionSandboxError):
    """A write was requested through a read-only workspace client."""
