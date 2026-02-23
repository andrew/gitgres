FROM postgres:17

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    libgit2-dev \
    libssl-dev \
    postgresql-server-dev-17 \
    ruby \
    ruby-dev \
    libpq-dev \
    git \
    && gem install pg --no-document \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /gitgres
COPY . .

RUN make backend && make ext && make -C ext install

COPY docker-init.sh /docker-entrypoint-initdb.d/01-gitgres.sh

ENV POSTGRES_DB=gitgres
ENV POSTGRES_HOST_AUTH_METHOD=trust
