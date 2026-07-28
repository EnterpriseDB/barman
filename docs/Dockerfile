# This Dockerfile resides in this path because it is the source docker image to build the
# documentation via github actions. There is no other use for it.

# Use Debian as the base image. uv manages the Python version as determined
# by requires-python in pyproject.toml, independently of the system.
FROM debian:trixie

# Switch to the root user
USER root

# Install necessary dependencies for Sphinx
RUN set -x \
  && apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y -o Acquire::Retries=10 --no-install-recommends \
  build-essential \
  libpq-dev \
  latexmk \
  tex-common \
  tex-gyre \
  texlive-base \
  texlive-binaries \
  texlive-fonts-recommended \
  texlive-latex-base \
  texlive-latex-extra \
  texlive-latex-recommended \
  texlive-pictures \
  texlive-plain-generic \
  git \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install uv to manage Python dependencies and run the docs build.
# Pinned to a specific version for reproducibility; update deliberately.
COPY --from=ghcr.io/astral-sh/uv:0.11.19 /uv /uvx /bin/

# Install Python to a shared location so any runtime UID can find it; without
# this, uv falls back to $HOME/.local/share/uv/python at runtime, which fails
# when the UID has no home directory inside the container.
ENV UV_PYTHON_INSTALL_DIR=/opt/uv/python
# Install Python into the image at build time so the docs build does not
# download it at runtime. The version matches requires-python in pyproject.toml.
RUN uv python install 3.12

# The container runs as the runner's UID at runtime, which has no entry in
# /etc/passwd and therefore no home directory. Pre-create the venv directory
# as root so any UID can populate it; without this, uv would fail trying to
# create /opt/venv because /opt is root-owned and not world-writable.
RUN mkdir -p /opt/venv && chmod 1777 /opt/venv

# Place the docs-build virtual environment inside the container rather than
# in the mounted project directory, so the host's .venv is not overwritten.
ENV UV_PROJECT_ENVIRONMENT=/opt/venv

# Without a home directory, uv falls back to /.cache/uv (root-owned),
# causing a permission denied error at startup. Disabling the cache avoids
# this entirely; it is also unnecessary since the container is ephemeral.
ENV UV_NO_CACHE=1
