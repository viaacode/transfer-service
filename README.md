# Transfer Service

## Synopsis

Lightweight service that will transfer files from the object store to a remote server.

We don't want any traffic in the service itself, so the file is transferred from the source URL to the server directly and not via the service in between. Connection to the remote server is via SSH and uses the higher-level SFTP protocol where possible. In the other cases, shell commands are executed.

There is an optional free space check in which the remote server needs to have a certain amount of free space (in percentage) before the file is transferred. If that free space is not met, the service will sleep indefinitely until the space is freed. The mountpoint is determined by the destination path of the target file. The free space of the threshold is defined by the ENV var: `SSH_FREE_SPACE_PERCENTAGE`. This var is mandatory but the value may be empty. In fact, if the value is empty then the check will not be executed, and the file will be transferred regardless of the free space on the target server.

## Prerequisites

- Git
- Docker (optional)
- Python 3.12+
- Access to the [meemoo PyPi](http://do-prd-mvn-01.do.viaa.be:8081)

## Diagrams

<details>
  <summary>Sequence diagram (click to expand)</summary>

  ![Transfer Service](http://www.plantuml.com/plantuml/proxy?src=https://raw.githubusercontent.com/viaacode/transfer-service/main/docs/transfer-service_sequence-diagram.plantuml&fmt=svg)

</details>

## Usage

1. Clone this repository with:

   `$ git clone https://github.com/viaacode/transfer-service.git`

2. Change into the new directory.

3. Set the needed config:

    Included in this repository is a `config.yml` file detailing the required configuration.
    There is also an `.env.example` file containing all the needed env variables used in the `config.yml` file.
    All values in the config have to be set in order for the application to function correctly.
    You can use `!ENV ${EXAMPLE}` as a config value to make the application get the `EXAMPLE` environment variable.

### Running locally

**Note**: As per the aforementioned requirements, this is a Python3
application. Check your Python version with `python --version`. You may want to
substitute the `python` command below with `python3` if your default Python version
is < 3.

1. Start by creating a virtual environment:

    `$ python -m venv env`

2. Activate the virtual environment:

    `$ source env/bin/activate`

3. Install the external modules:

    ```
    $ pip install -r requirements.txt \
        --extra-index-url http://do-prd-mvn-01.do.viaa.be:8081/repository/pypi-all/simple \
        --trusted-host do-prd-mvn-01.do.viaa.be
    ```

4. Run the tests (and set the values in `.env.example`) with:

    To be able to run the tests, the test dependencies need to be installed as well:

    ```
    $ pip install -r requirements-test.txt
    ```

    Then run:

    `$ export $(grep -v '^#' .env.example | xargs); pytest -v --cov=./app`

5. Run the application:

    `$ python main.py`

### Running using Docker

1. Build the container:

   `$ docker build -t transfer-service:latest .`

2. Run the test container:

   `$ docker run --env-file .env.example --rm --entrypoint python transfer-service:latest -m pytest -v --cov=./app`

2. Run the container (with specified `.env` file):

   `$ docker run --env-file .env --rm transfer-service:latest`
