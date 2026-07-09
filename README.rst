Earth Engine Client 🌎
==============================

.. image:: https://github.com/dfguerrerom/ee-client/actions/workflows/tests.yaml/badge.svg?branch=main
    :target: https://github.com/dfguerrerom/ee-client/actions/workflows/tests.yaml
    :alt: Tests

.. image:: https://img.shields.io/pypi/v/ee-client.svg
    :target: https://pypi.org/project/ee-client/
    :alt: PyPI version

.. image:: https://img.shields.io/pypi/pyversions/ee-client.svg
    :target: https://pypi.org/project/ee-client/
    :alt: Python versions

.. image:: https://img.shields.io/conda/vn/conda-forge/ee-client.svg
    :target: https://anaconda.org/conda-forge/ee-client
    :alt: Conda-Forge version

.. image:: https://img.shields.io/pypi/l/ee-client.svg
    :target: https://github.com/dfguerrerom/ee-client/blob/main/LICENSE
    :alt: License

The **Earth Engine Session Client** is a Python package that extends the Google Earth Engine (GEE) API by introducing multi-user session management through custom authentication.

**Why This Package?**

While Google Earth Engine applications can be created using a global service account, this approach has significant limitations: users cannot access their private GEE assets without making them public. This package solves this problem by handling custom authentication, allowing each user to access their own private assets securely.

Unlike the standard GEE API—which relies on a global session object and does not support multi-user environments—this client ensures that each session is authenticated and managed independently with user-specific credentials.

Each session is instantiated via the ``EESession`` class, which supports several credential sources — a service-account key, an ``EARTHENGINE_TOKEN``, ``earthengine authenticate`` OAuth credentials, or Application Default Credentials. Once authenticated, the session exposes an ``operations`` property that provides easy access to key API methods.

Key Features
------------

- **Custom User Authentication**: Enable users to access their private GEE assets without requiring them to be public, solving the limitation of global service account approaches.
- **Multi-User Session Management**: Encapsulate user-specific credentials and project data in independent ``EESession`` objects.
- **Enhanced API Operations**: Access GEE functionalities via the ``operations`` property, which includes methods such as:
  - ``get_info``: Retrieve detailed information about an Earth Engine object.
  - ``get_map_id``: Generate a map ID for an Earth Engine image.
  - ``get_asset``: Fetch information about a specific Earth Engine asset.
- **Seamless GEE Integration**: Integrate custom methods into your existing Earth Engine workflow with minimal changes.

Installation
------------

To install the package, simply use pip:

.. code-block:: bash

   pip install ee-client

Usage
-----

Initialization and Authentication
+++++++++++++++++++++++++++++++++

A session can be built from any standard Google/Earth Engine credential via factory constructors, each holding a live, refreshable credential:

.. code-block:: python

   from eeclient import EESession

   # Service-account key (dict or path)
   session = EESession.from_service_account("sa-key.json")

   # EARTHENGINE_TOKEN env var, else ~/.config/earthengine/credentials
   session = EESession.from_earthengine_token()

   # An existing google.auth Credentials object
   session = EESession.from_google_credentials(creds, project="my-project")

   # Application Default Credentials (explicit opt-in)
   session = EESession.from_application_default()

   # Resolve from the environment (opt-in): EARTHENGINE_TOKEN / EE OAuth file
   session = EESession.from_default()

Credentials are **never resolved implicitly** — use a ``from_*`` factory to build a session. To resolve from the environment explicitly, call ``EESession.from_default()``, which walks local sources only, in this order:

1. ``EARTHENGINE_TOKEN`` environment variable
2. Earth Engine OAuth file (``~/.config/earthengine/credentials``)

Application Default Credentials are **not** included — call ``EESession.from_application_default()`` to use them.

To see which source a session ended up using, inspect ``session.auth_mode`` (the credential *kind*: ``file``/``oauth``/``service_account``/``adc``) and ``session.auth_source`` (the precise origin: ``earthengine_token``/``ee_oauth_file``/``service_account``/``application_default``/``google_credentials``).

Making API Calls
++++++++++++++++

After initializing the session, use the ``operations`` property to access the key GEE methods. For example, you can retrieve information about Earth Engine objects, generate map IDs, or fetch asset details:

.. code-block:: python

   import ee

   # Initialize the Earth Engine library (this can use any authentication method/account)
   # The purpose of this is to ensure the ee library is available for use
   ee.Initialize()

   # Use the operations available in the session
   result_info = session.operations.get_info(ee.Number(5))
   print(result_info) # the GEE server call is done using the custom EE client

   # Example: Generate a map ID for an Earth Engine image
   image = ee.Image('COPERNICUS/S2/20190726T104031_20190726T104035_T31TGL')
   map_id = session.operations.get_map_id(image)
   print(map_id)

   # Example: Retrieve asset information
   asset_info = session.operations.get_asset("users/your_username/your_asset")
   print(asset_info)


Contributing
------------

We welcome contributions from the community. If you wish to help improve this package, please submit issues or pull requests.

Forking and Branching
+++++++++++++++++++++

1. Fork the repository.
2. Create a new branch:

   .. code-block:: bash

      git checkout -b feature-branch

3. Commit your changes:

   .. code-block:: bash

      git commit -am 'Add new feature'

4. Push the branch:

   .. code-block:: bash

      git push origin feature-branch

5. Create a new Pull Request.

License
-------

This project is licensed under the MIT License. See the LICENSE file for details.
