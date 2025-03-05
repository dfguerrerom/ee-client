Earth Engine Client 🌎
==============================

The **Earth Engine Session Client** is a Python package that extends the Google Earth Engine (GEE) API by introducing multi-user session management through a custom authentication. Unlike the standard GEE API—which relies on a global session object and does not support multi-user environments—this client ensures that each session is authenticated and managed independently.

Each session is instantiated via the ``EESession`` class, currently only accepts SEPAL headers as its only parameter. **A valid ``sepal-session-id`` cookie must be present in these headers**, as it is used to retrieve the corresponding GEE credentials. Once authenticated, the session exposes an ``operations`` property that provides easy access to key API methods.

Key Features
------------

- **SEPAL-based Initialization**: Create sessions using SEPAL headers. The required ``sepal-session-id`` cookie is automatically used to retrieve GEE credentials.
- **Custom Session Management**: Encapsulate user-specific credentials and project data in an ``EESession`` object.
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

The Earth Engine Session Client must be initialized using SEPAL headers. **Ensure that the headers include the ``sepal-session-id`` cookie**, which is essential for retrieving the GEE credentials.

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
