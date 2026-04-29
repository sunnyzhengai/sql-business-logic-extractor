"""Soft license / feature-gate module.

Each of the 4 tools (and their LLM-enhanced variants) checks for a feature
flag at the entry of its public function. Feature flags come from one of
three sources, in priority order:

1. SLE_LICENSE_FILE environment variable -- path to a signed key file
   (real implementation deferred to August commercialisation).
2. SLE_FEATURES environment variable -- comma-separated feature list, used
   for testing and ops overrides.
3. Dev default -- every feature enabled, used during local development and
   tests.

Healthcare-safe deployments unlock only `columns`, `technical_logic`,
`business_logic`, `report_description`. They do NOT unlock the `*_llm`
features, and the LLM client libraries (google-genai etc.) are lazy-
imported inside the LLM code paths only -- so a no-LLM build doesn't even
have those packages installed.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


class LicenseError(RuntimeError):
    """Raised when the active license doesn't include the requested feature."""


_ALL_FEATURES = frozenset({
    # Tool-level access
    "columns",                      # Tool 1
    "technical_logic",              # Tool 2
    "business_logic",               # Tool 3
    "report_description",           # Tool 4
    # LLM-enhancement add-ons (separate gates so a license can grant tool
    # access without also granting LLM mode for that tool).
    "business_logic_llm",
    "report_description_llm",
})


@dataclass(frozen=True)
class License:
    """The set of features unlocked for the current process."""
    customer_id: str
    enabled_features: frozenset[str]
    expiry: str | None = None


_active: License | None = None


def current_license() -> License:
    """Return the active License. Resolved once per process and cached."""
    global _active
    if _active is not None:
        return _active

    license_file = os.environ.get("SLE_LICENSE_FILE")
    feature_override = os.environ.get("SLE_FEATURES")

    if license_file:
        # TODO (August): parse + verify signed key file. Until then this
        # path is intentionally not implemented to surface attempted prod
        # use without a real loader.
        raise NotImplementedError(
            "Signed-license file loader is a TODO for August commercialisation. "
            "For now, set SLE_FEATURES=<comma,separated,list> instead."
        )
    if feature_override is not None:
        features = frozenset(f.strip() for f in feature_override.split(",") if f.strip())
        _active = License(customer_id="env", enabled_features=features)
    else:
        # Dev default: every feature enabled. Lets tests and local runs
        # work without any setup. Production uses one of the explicit
        # paths above.
        _active = License(customer_id="dev", enabled_features=_ALL_FEATURES)
    return _active


def require_feature(feature: str) -> None:
    """Raise LicenseError if the active license doesn't include `feature`."""
    if feature not in current_license().enabled_features:
        raise LicenseError(
            f"Your license doesn't include '{feature}'. "
            "Visit your account page to upgrade."
        )


def reset_license_cache() -> None:
    """Test helper -- forces the license to be re-resolved on next access."""
    global _active
    _active = None
