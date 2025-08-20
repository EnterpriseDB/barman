# This Dockerfile resides in this path because it is the source docker image to build the
# documentation via github actions. There is no other use for it.

# Use Debian as the base image
FROM debian:latest

# Switch to the root user
USER root

# Install necessary dependencies for Sphinx
RUN set -x \
  && apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y -o Acquire::Retries=10 --no-install-recommends \
  build-essential \
  libpq-dev \
  python3 \
  python3-dev \
  python3-venv \
  python3-pip \
  python3-setuptools \
  python3-sphinx \
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

# Set sphinx version

# Create virtual env to install python dependencies
RUN python3 -m venv /opt/venv

# Add virtual env bin path to PATH
ENV PATH="/opt/venv/bin:$PATH"

# Install Sphinx and any additional Python dependencies
RUN pip3 install --upgrade pip \
  && pip3 install tox
