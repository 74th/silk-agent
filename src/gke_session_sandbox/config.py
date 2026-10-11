"""Validated manager/runtime settings and reserved-field protection."""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from pydantic import BaseModel, ConfigDict, Field, SecretStr, StrictBool, StrictInt, StrictStr, field_validator, model_validator

from .errors import TemplateValidationError


_QUANTITY_RE = re.compile(
    r"^(?P<number>[+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?)(?P<suffix>Ki|Mi|Gi|Ti|Pi|Ei|n|u|m|k|K|M|G|T|P|E)?$"
)
_SUFFIX_FACTORS = {
    "n": Decimal("1e-9"), "u": Decimal("1e-6"), "m": Decimal("1e-3"),
    "k": Decimal(1000), "K": Decimal(1000), "M": Decimal(1000) ** 2,
    "G": Decimal(1000) ** 3, "T": Decimal(1000) ** 4, "P": Decimal(1000) ** 5,
    "E": Decimal(1000) ** 6, "Ki": Decimal(1024), "Mi": Decimal(1024) ** 2,
    "Gi": Decimal(1024) ** 3, "Ti": Decimal(1024) ** 4,
    "Pi": Decimal(1024) ** 5, "Ei": Decimal(1024) ** 6,
}

RESERVED_MANIFEST_PATHS: frozenset[tuple[str, ...]] = frozenset(
    {
        ("metadata", "name"),
        ("metadata", "namespace"),
        ("metadata", "uid"),
        ("metadata", "ownerReferences"),
        ("metadata", "labels", "session-sandbox.example.com/managed"),
        ("metadata", "labels", "session-sandbox.example.com/session-key-hash"),
        ("metadata", "labels", "session-sandbox.example.com/generation"),
        ("spec", "operatingMode"),
        ("spec", "shutdownTime"),
        ("spec", "shutdownPolicy"),
    }
)


def parse_quantity(value: str) -> Decimal:
    """Validate a positive Kubernetes resource quantity and return its base value."""
    if not isinstance(value, str):
        raise ValueError("quantity must be a string")
    match = _QUANTITY_RE.fullmatch(value)
    if match is None:
        raise ValueError("must be a valid Kubernetes resource quantity")
    try:
        number = Decimal(match.group("number"))
    except InvalidOperation as exc:
        raise ValueError("must be a valid Kubernetes resource quantity") from exc
    quantity = number * _SUFFIX_FACTORS.get(match.group("suffix") or "", Decimal(1))
    if not quantity.is_finite() or quantity <= 0:
        raise ValueError("quantity must be finite and greater than zero")
    return quantity


def reject_reserved_manifest_fields(value: Mapping[str, Any]) -> None:
    """Reject any configured manifest subtree that writes library-owned fields."""
    def visit(node: Mapping[str, Any], prefix: tuple[str, ...] = ()) -> None:
        for key, child in node.items():
            path = (*prefix, str(key))
            if path in RESERVED_MANIFEST_PATHS:
                dotted = ".".join(path)
                raise TemplateValidationError(f"template override targets reserved field {dotted!r}")
            if isinstance(child, Mapping):
                visit(child, path)

    visit(value)


class StrictSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_assignment=True)


class RuntimeSettings(StrictSettings):
    container_name: StrictStr = Field(default="sandboxd", min_length=1)
    image: StrictStr = Field(min_length=1)
    rest_port: StrictInt = Field(default=8080, ge=1, le=65535)
    tracker_port: StrictInt = Field(default=8081, ge=1, le=65535)
    grpc_port: StrictInt = Field(default=9090, ge=1, le=65535)
    cpu_request: StrictStr = "100m"
    cpu_limit: StrictStr = "1"
    memory_request: StrictStr = "128Mi"
    memory_limit: StrictStr = "1Gi"

    @field_validator("cpu_request", "cpu_limit", "memory_request", "memory_limit")
    @classmethod
    def valid_quantity(cls, value: str) -> str:
        parse_quantity(value)
        return value

    @field_validator("container_name")
    @classmethod
    def container_name_is_dns_label(cls, value: str) -> str:
        if len(value) > 63 or re.fullmatch(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?", value) is None:
            raise ValueError("container_name must be a Kubernetes DNS label")
        return value

    @model_validator(mode="after")
    def listener_ports_are_distinct(self) -> "RuntimeSettings":
        ports = (self.rest_port, self.tracker_port, self.grpc_port)
        if len(set(ports)) != len(ports):
            raise ValueError("runtime listener ports must be distinct")
        for request_name, limit_name in (("cpu_request", "cpu_limit"), ("memory_request", "memory_limit")):
            if parse_quantity(getattr(self, request_name)) > parse_quantity(getattr(self, limit_name)):
                raise ValueError(f"{request_name} must not exceed {limit_name}")
        return self


class LifecycleSettings(StrictSettings):
    reconcile_interval_seconds: StrictInt = Field(default=60, gt=0)
    idle_timeout_seconds: StrictInt = Field(default=300, gt=0)
    retention_seconds: StrictInt = Field(default=604800, gt=0)
    command_output_limit_bytes: StrictInt = Field(default=64 * 1024 * 1024, gt=0)
    session_output_limit_bytes: StrictInt = Field(default=256 * 1024 * 1024, gt=0)
    buffered_response_limit_bytes: StrictInt = Field(default=8 * 1024 * 1024, gt=0)

    @model_validator(mode="after")
    def response_limit_does_not_exceed_storage_limits(self) -> "LifecycleSettings":
        if self.buffered_response_limit_bytes > self.command_output_limit_bytes:
            raise ValueError("buffered_response_limit_bytes must not exceed command_output_limit_bytes")
        if self.command_output_limit_bytes > self.session_output_limit_bytes:
            raise ValueError("command_output_limit_bytes must not exceed session_output_limit_bytes")
        return self


class NetworkPort(StrictSettings):
    protocol: StrictStr = "TCP"
    port: StrictInt = Field(default=443, ge=1, le=65535)

    @field_validator("protocol")
    @classmethod
    def valid_protocol(cls, value: str) -> str:
        if value not in {"TCP", "UDP"}:
            raise ValueError("network policy protocol must be TCP or UDP")
        return value


class AllowedFQDN(StrictSettings):
    name: StrictStr | None = None
    pattern: StrictStr | None = None
    ports: tuple[NetworkPort, ...] = Field(default_factory=lambda: (NetworkPort(),))

    @model_validator(mode="after")
    def exactly_one_match(self) -> "AllowedFQDN":
        if (self.name is None) == (self.pattern is None):
            raise ValueError("specify exactly one of name or pattern")
        fqdn = self.name or self.pattern or ""
        if not fqdn or len(fqdn) > 253:
            raise ValueError("FQDN match must contain between 1 and 253 characters")
        if "*" in fqdn and self.name is not None:
            raise ValueError("wildcards must use pattern, not name")
        if not self.ports:
            raise ValueError("each FQDN rule must allow at least one explicit protocol/port")
        if len({(entry.protocol, entry.port) for entry in self.ports}) != len(self.ports):
            raise ValueError("FQDN rule ports must be unique")
        return self


class NetworkSettings(StrictSettings):
    allowed_fqdns: tuple[AllowedFQDN, ...] = ()
    dns_namespace: StrictStr = "kube-system"
    dns_pod_selector: dict[StrictStr, StrictStr] = Field(default_factory=lambda: {"k8s-app": "kube-dns"})

    @field_validator("dns_namespace")
    @classmethod
    def dns_namespace_is_dns_label(cls, value: str) -> str:
        if len(value) > 63 or re.fullmatch(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?", value) is None:
            raise ValueError("dns_namespace must be a Kubernetes DNS label")
        return value


class ManagerConfig(StrictSettings):
    namespace: StrictStr = "session-sandboxes"
    control_namespace: StrictStr = "session-sandbox-system"
    template_file: StrictStr = "templates/sandbox.yaml"
    workspace_storage_class: StrictStr = "session-workspace-rwo"
    workspace_storage: StrictStr = "10Gi"
    shared_code_claim: StrictStr | None = None
    runtime: RuntimeSettings
    lifecycle: LifecycleSettings = Field(default_factory=LifecycleSettings)
    network: NetworkSettings = Field(default_factory=NetworkSettings)
    template_overrides: dict[str, Any] = Field(default_factory=dict)
    environment_fingerprint_key: SecretStr | None = None
    repository_database_path: StrictStr = "/var/lib/session-sandbox/sessions.sqlite3"
    repository_require_mount: StrictBool = True
    max_pending_events: StrictInt = Field(default=100, gt=0)
    session_lease_duration_seconds: StrictInt = Field(default=60, gt=3)
    lease_acquire_timeout_seconds: StrictInt = Field(default=300, gt=0)

    @field_validator("repository_database_path")
    @classmethod
    def repository_database_path_is_set(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("repository_database_path must not be blank")
        return value

    @field_validator("namespace", "control_namespace", "workspace_storage_class")
    @classmethod
    def kubernetes_name_is_dns_label(cls, value: str) -> str:
        if len(value) > 63 or re.fullmatch(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?", value) is None:
            raise ValueError("must be a Kubernetes DNS label")
        return value

    @field_validator("workspace_storage")
    @classmethod
    def workspace_quantity_is_valid(cls, value: str) -> str:
        parse_quantity(value)
        return value

    @field_validator("template_overrides")
    @classmethod
    def template_fields_are_not_reserved(cls, value: dict[str, Any]) -> dict[str, Any]:
        reject_reserved_manifest_fields(value)
        return value

    @field_validator("environment_fingerprint_key")
    @classmethod
    def fingerprint_key_is_sufficiently_random(cls, value: SecretStr | None) -> SecretStr | None:
        if value is not None and len(value.get_secret_value().encode("utf-8")) < 32:
            raise ValueError("environment_fingerprint_key must contain at least 32 bytes")
        return value
