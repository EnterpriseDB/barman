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

# Install Python into the image at build time so the docs build does not
# download it at runtime. The version matches requires-python in pyproject.toml.
RUN uv python install 3.12

# Place the docs-build virtual environment inside the container rather than
# in the mounted project directory, so the host's .venv is not overwritten.
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
