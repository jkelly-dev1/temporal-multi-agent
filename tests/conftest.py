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

import pytest

from multi_agent import providers

# Every environment variable the Anthropic SDK will accept as a credential.
# The gate in get_provider() only reads the first one, which is the safe
# direction, since an unrecognized credential falls back to the mock. The
# suite still clears all of them, so the floor does not depend on which one
# the gate happens to look at.
_CREDENTIAL_VARS = ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN")


@pytest.fixture(autouse=True)
def force_the_offline_mock(monkeypatch):
    """Pin AGENT_PROVIDER=mock and clear every credential, for every test."""
    monkeypatch.setenv("AGENT_PROVIDER", "mock")
    for var in _CREDENTIAL_VARS:
        monkeypatch.delenv(var, raising=False)
    # The provider is a process-wide singleton, so a value cached under some
    # other environment would outlive the setenv above.
    providers._PROVIDER = None
    yield
    providers._PROVIDER = None
