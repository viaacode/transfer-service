FROM python:3.8-slim

# Make a new group and user so we don't run as root.
RUN addgroup --system appgroup && adduser --system appuser --ingroup appgroup

WORKDIR /app

# Install curl
RUN apt-get update
RUN apt-get install -y --no-install-recommends curl openssh-client

# Install Poetry
RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | POETRY_HOME=/opt/poetry python && \
    cd /usr/local/bin && \
    ln -s /opt/poetry/bin/poetry && \
    poetry config virtualenvs.create false

# Let the appuser own the files so he can rwx during runtime.
COPY . .
RUN chown -R appuser:appgroup /app

# Install Python dependencies.
RUN poetry install --no-root

USER appuser

# This command will be run when starting the container. It is the same one that
# can be used to run the application locally.
CMD [ "python", "main.py"]