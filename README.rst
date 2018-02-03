Moisturizer
===========

**Under development!**


Installing Cassandra
--------------------

Using docker:

.. code-block:: bash

    docker pull cassandra
    docker run --name cassandra-server \
        -p 127.0.0.1:9042:9042 \
        -p 127.0.0.1:9160:9160 \
        -d cassandra


Running locally
---------------

The recommended way to running this locally is using a Python 3 virtualenv.

.. code-block:: bash

    virtualenv .venv --python=python3
    source .venv/bin/activate


Next, install and run the server with ``pserve``.

.. code-block:: bash

    pip install -r requirements.txt
    python setup.py develop
    pserve moisturizer.ini


Testing
-------

.. code-block:: bash

    make install-dev
    make tests-once  # Run local Python version
    make tests  # Run full suite
