from typing import Any

import neo4j
from flask import Flask, current_app
# tag::import[]
from neo4j import GraphDatabase

# end::import[]

"""
Initiate the Neo4j Driver
"""


# tag::initDriver[]
def init_driver(uri: str, username: Any, password: Any) -> neo4j.Driver:
    """
    Create a driver object and verify the connection to the database.

    :param uri: The Universal Resource Identifier (URI) of the connection
    :type uri: str
    :param username: The username to use in the authentication details
    :type username: Any
    :param password: The password associated with the :param:`username` to use
        in the authentication details
    :type password: Any
    :return: An instance of a Neo4j Driver connected via the input :param:`uri`
        and that uses the :param:`username` and :param:`password` as
        authentication credentials
    :rtype: neo4j.Driver
    """
    current_app.driver = GraphDatabase.driver(uri=uri, auth=(username, password))
    current_app.driver.verify_connectivity()

    return current_app.driver


# end::initDriver[]


"""
Get the instance of the Neo4j Driver created in the `initDriver` function
"""


# tag::getDriver[]
def get_driver():
    return current_app.driver


# end::getDriver[]

"""
If the driver has been instantiated, close it and all remaining open sessions
"""


# tag::closeDriver[]
def close_driver():
    if current_app.driver != None:
        current_app.driver.close()
        current_app.driver = None

        return current_app.driver
# end::closeDriver[]
