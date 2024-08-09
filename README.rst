Earth Engine Session Client
===========================

ee-client extends the capabilities of the earthengine-api by providing custom session management and client interactions


ee-client is a Python package designed to extend the capabilities of the Google Earth Engine (GEE) API by providing custom session management and client interactions. This package allows users to make authenticated requests to the Earth Engine REST API using their own credentials, facilitating seamless integration and customization of GEE functionalities.

Key Features
------------

- Custom Authentication: 
- Session Management: Handle sessions efficiently with reusable session objects that store credentials and project information.
- Enhanced API Calls: Replace and extend existing GEE API calls with custom methods to suit specific project requirements.
- Integration with Existing GEE Objects: Seamlessly integrate custom methods into existing Earth Engine objects, allowing for intuitive and familiar usage.


Installation
------------

To install the package, simply use pip:

.. code-block:: bash

    pip install ee-client # Not yet developed

Usage
-----

Here are a few examples of how to use ee-client in your projects:

Initialization and Authentication
+++++++++++++++++++++++++++++++++

.. code-block:: python

    from eeclient import Session, get_info, get_asset

    # Define your credentials and project ID
    credentials = {
        "client_id": "your_client_id",
        "client_secret": "your_client_secret",
        "refresh_token": "your_refresh_token",
    }
    project = "your_project"

    # Create a session object
    session = Session(credentials, project)


Making API Calls
++++++++++++++++

.. code-block:: python

    import ee

    # Initialize the Earth Engine library and authenticate
    ee.Initialize()

    # Example: Get information about an Earth Engine object
    result = get_info(session, ee.Number(5))
    print(result)

    # Example: Get asset information
    asset_info = get_asset(session, "users/your_username/your_asset")
    print(asset_info)

Fetching Map Tiles:
+++++++++++++++++++

.. code-block:: python
    
    from eeclient import get_map_id, get_map_tile

    # Example: Get map ID for an Earth Engine image
    image = ee.Image('COPERNICUS/S2/20190726T104031_20190726T104035_T31TGL')
    map_id = get_map_id(session, image)

    # Example: Get map tile layer
    tile_layer = get_map_tile(map_id)
    print(tile_layer)

Integration with Existing GEE Objects
+++++++++++++++++++++++++++++++++++++

WIP

.. code-block:: python

    import ee
    import eeclient

    # Custom method to get information about an Earth Engine Number object
    def custom_get_info(self, session):
        return get_info(session, self)

    # Extend the Earth Engine Number class with the custom method
    ee.Number.custom_get_info = custom_get_info

    # Usage
    number = ee.Number(5)
    result = number.eeclient.get_info(session)
    print(result)


Contributing
------------

We welcome contributions from the community. Please feel free to submit issues and pull requests to help improve this package.

Fork the repository
+++++++++++++++++++

Create a new branch (git checkout -b feature-branch).
Commit your changes (git commit -am 'Add new feature').
Push to the branch (git push origin feature-branch).
Create a new Pull Request.

License
-------
This project is licensed under the MIT License - see the LICENSE file for details.

