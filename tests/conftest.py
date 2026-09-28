"""Suite-wide guarantees that have to hold before any individual test runs.

The suite must not be able to spend money. `get_provider()` selects the real
Anthropic path when AGENT_PROVIDER=anthropic and a credential are both present
in the environment, and the README's own "use a real LLM" recipe tells the
reader to export exactly that pair. Without a floor, a bare `pytest` in that
shell would make live billed calls on every research and synthesis activity.

The environment is therefore forced here instead of trusted. The fixture is
autouse and function-scoped, because `TestProviderSelection` manipulates the
same two variables and a function-scoped fixture re-establishes the floor
before the next test.
"""

import os

import pytest

from multi_agent import providers

# The gate in get_provider() builds the real provider only when the API key
# variable is set. The SDK itself accepts more than that (an auth token,
# profiles, workload identity), so the fixture clears every variable with the
# SDK's prefix, and the floor does not depend on which one the gate looks at.
# Profile files on disk are not touched; the gate never reads them.
_CREDENTIAL_PREFIX = "ANTHROPIC_"


@pytest.fixture(autouse=True)
def force_the_offline_mock(monkeypatch):
    """Pin AGENT_PROVIDER=mock and clear every ANTHROPIC_* variable."""
    monkeypatch.setenv("AGENT_PROVIDER", "mock")
    for var in [v for v in os.environ if v.startswith(_CREDENTIAL_PREFIX)]:
        monkeypatch.delenv(var, raising=False)
    # The provider is a process-wide singleton, so a value cached under some
    # other environment would outlive the setenv above.
    providers._PROVIDER = None
    yield
    providers._PROVIDER = None
