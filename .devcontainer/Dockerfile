# Why this image is built rather than used directly:
#
# mcr.microsoft.com/devcontainers/python:1-3.11-bookworm ships an apt source for
# dl.yarnpkg.com whose signing key has since rotated. Any `apt-get update` inside
# the image now fails with:
#
#   W: GPG error: https://dl.yarnpkg.com/debian stable InRelease: the following
#      signatures couldn't be verified: NO_PUBKEY 62D54FD4003F6525
#
# Dev Container Features install their packages with apt, so the failure takes
# down the docker-in-docker feature (exit code 100), which fails the whole
# container build. Codespaces then silently falls back to a `base:alpine`
# recovery container — no Python 3.11, no JDK, no Docker, and postCreateCommand
# never runs, so the failure looks like "the setup script didn't fire".
#
# Removing the stale source before Features are layered on top fixes it. Yarn is
# not used by this project.

FROM mcr.microsoft.com/devcontainers/python:1-3.11-bookworm

USER root

RUN set -eux; \
    # Drop any apt source pointing at yarnpkg, whatever it is named.
    find /etc/apt/sources.list.d/ -type f \
        -exec grep -lq 'dl\.yarnpkg\.com' {} \; -delete 2>/dev/null || true; \
    sed -i '/dl\.yarnpkg\.com/d' /etc/apt/sources.list 2>/dev/null || true; \
    # Prove the fix inside the build, so a future key rotation elsewhere fails
    # here with a clear message instead of inside an opaque Feature install.
    apt-get update; \
    rm -rf /var/lib/apt/lists/*

USER vscode
