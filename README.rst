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

Each session is instantiated via the ``EESession`` class, which supports several credential sources — SEPAL headers, a service-account key, an ``EARTHENGINE_TOKEN``, ``earthengine authenticate`` OAuth credentials, or Application Default Credentials. SEPAL is one supported source among several. Once authenticated, the session exposes an ``operations`` property that provides easy access to key API methods.

Key Features
------------

- **Custom User Authentication**: Enable users to access their private GEE assets without requiring them to be public, solving the limitation of global service account approaches.
- **SEPAL-based Initialization**: Create sessions using SEPAL headers. The required ``sepal-session-id`` cookie is automatically used to retrieve GEE credentials.
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

The Earth Engine Session Client can be initialized from SEPAL headers (shown here) or from any standard Google/Earth Engine credential source (see *Credential sources* below). For the SEPAL path, **ensure that the headers include the ``sepal-session-id`` cookie**, which is used to retrieve the GEE credentials.

.. code-block:: python

   from eeclient import EESession

   # Example SEPAL headers with the mandatory sepal-session-id cookie.
   sepal_headers = {
       "cookie": [
           "sepal-session-id=your_session_id",
           "other_cookie=other_value"
       ],
       "sepal_user": [{
           "id": 123,
           "username": "your_username",
           "googleTokens": {
               "accessToken": "your_access_token",
               "refreshToken": "your_refresh_token",
               "accessTokenExpiryDate": 1234567890,
               "REFRESH_IF_EXPIRES_IN_MINUTES": 10,
               "projectId": "your_project_id",
               "legacyProject": "your_legacy_project"
           },
           "status": "active",
           "roles": ["role1", "role2"],
           "systemUser": False,
           "admin": False
       }]
   }

   # Create and validate the session with SEPAL headers
   session = EESession(sepal_headers)

Credential sources
++++++++++++++++++

Beyond SEPAL headers, a session can be built from any standard Google/Earth Engine credential via factory constructors, each holding a live, refreshable credential:

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

   # Resolve from the environment (opt-in): SEPAL file / EARTHENGINE_TOKEN / EE OAuth file
   session = EESession.from_default()

Credentials are **never resolved implicitly** — a bare ``EESession()`` requires a source (pass ``sepal_headers`` or use a ``from_*`` factory). To resolve from the environment explicitly, call ``EESession.from_default()``, which walks local sources only, in this order:

1. ``EARTHENGINE_TOKEN`` environment variable
2. Earth Engine OAuth file (``~/.config/earthengine/credentials``)

Application Default Credentials are **not** included — call ``EESession.from_application_default()`` to use them. Inside a SEPAL environment (a ``sepal-user`` home) ``from_default()`` is SEPAL-only and fails closed.

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
