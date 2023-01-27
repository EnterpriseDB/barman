FROM debian:stretch
USER root
# Install all the required packages for the pandoc stack.
RUN set -x \
  && apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y -o Acquire::Retries=10 --no-install-recommends \
    make \
    texlive-latex-base \
    texlive-xetex latex-xcolor \
    texlive-math-extra \
    texlive-latex-extra \
    texlive-fonts-extra \
    texlive-bibtex-extra \
    fontconfig \
    lmodern \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
ENV PKGREL 1
ENV VERSION 2.2.1
ADD https://github.com/jgm/pandoc/releases/download/${VERSION}/pandoc-${VERSION}-${PKGREL}-amd64.deb /pandoc.deb
RUN set +x \
    && DEBIAN_FRONTEND=noninteractive dpkg -i /pandoc.deb \
    && rm -f /pandoc.deb
## Add a jenkins user to avoid issues
#RUN useradd -ms /bin/bash jenkins
#USER jenkins
#ENV USER jenkins
