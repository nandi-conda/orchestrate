"""The models a project's threads can run on (Project settings, Model), each
with the provider a worker asks for it and the key that provider needs.

runner/models.json declares each of them to prime-agent, which runs
--offline and knows no others; the runner refuses a thread on one it does
not declare, or whose key it was not given (PrimeAgent.check_model). The
control plane passes every key it has to the workers it starts
(providers.passthrough_env).
"""

from __future__ import annotations

# name -> (provider, the key it needs). "" is the runner's default, deepseek-flash.
MODELS = {
    "deepseek-flash": ("deepseek", "DEEPSEEK_API_KEY"),
    "deepseek-v4-pro": ("deepseek", "DEEPSEEK_API_KEY"),
    "deepseek/deepseek-v4.1-flash-ultrafast": ("opengateway", "OPENGATEWAY_API_TOKEN"),
}


def check_name(model: str) -> None:
    """ValueError unless `model` is "" or one of MODELS."""
    if model and model not in MODELS:
        raise ValueError(f"no model {model!r}; a project runs on " + ", ".join(MODELS))


def check_key(model: str) -> None:
    """ValueError when this computer has not got the key `model`'s provider
    needs: its workers would be refused every thread."""
    from .secrets import get_secret, missing_message

    check_name(model)
    if model and not get_secret(MODELS[model][1]):
        raise ValueError(f"{model} needs {MODELS[model][1]}.\n\n" + missing_message([MODELS[model][1]]))


def worker_model(model: str) -> dict | None:
    """What a runner is posted for a project on `model` (POST /threads
    `model`), None for the runner's default."""
    check_name(model)
    return {"provider": MODELS[model][0], "model": model} if model else None
