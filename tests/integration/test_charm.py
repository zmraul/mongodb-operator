#!/usr/bin/env python3
# Copyright 2021 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from pathlib import Path
import os
import pytest
import yaml
from pytest_operator.plugin import OpsTest
from tests.integration.helpers import pull_content_from_unit_file
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
UNIT_IDS = [0, 1, 2]
PORT = 27017


# @pytest.mark.skipif(
#     os.environ.get('PYTEST_SKIP_DEPLOY', False),
#     reason="skipping deploy, model expected to be provided"
# )

@pytest.mark.skipif(
    os.environ.get('PYTEST_SKIP_DEPLOY', False)
)
@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest) -> None:
    """Build and deploy one unit of MongoDB"""
    my_charm = await ops_test.build_charm(".")
    await ops_test.model.deploy(my_charm, num_units=len(UNIT_IDS))
    await ops_test.model.wait_for_idle()


@pytest.mark.abort_on_fail
async def test_status(ops_test: OpsTest) -> None:
    """Verifies that the application and unit are active"""
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], status="active", timeout=1000
    )
    assert len(ops_test.model.applications[APP_NAME].units) == len(UNIT_IDS)


@pytest.mark.parametrize("unit_id", UNIT_IDS)
async def test_config_files_are_correct(ops_test: OpsTest, unit_id: int) -> None:
    """Tests that mongo.conf as expected content"""
    # Get the expected contents from files.
    with open("tests/data/mongod.conf") as file:
        expected_mongodb_conf = file.read()

    # Pull the configuration files from MongoDB instance.
    unit = ops_test.model.applications[f"{APP_NAME}"].units[unit_id]

    # Check that the conf settings are as expected.
    unit_mongodb_conf_data = await pull_content_from_unit_file(
        unit, "/etc/mongod.conf"
    )
    expected_mongodb_conf = update_bind_ip(
        expected_mongodb_conf, unit.public_address
    )
    assert expected_mongodb_conf == unit_mongodb_conf_data


@pytest.mark.parametrize("unit_id", UNIT_IDS)
async def test_unit_is_running_as_replica_set(ops_test: OpsTest, unit_id: int) -> None:
    """Tests that mongodb is running as a replica set for the application unit
    """
    # connect to mongo replicaSet
    unit = ops_test.model.applications[APP_NAME].units[unit_id]
    connection = unit.public_address + ":" + str(PORT)
    client = MongoClient(connection, replicaset="rs0")

    # check mongo replicaset is ready
    try:
        client.server_info()
    except ServerSelectionTimeoutError:
        assert False, "server is not ready"

    # close connection
    client.close()


async def test_all_units_in_same_replica_set(ops_test: OpsTest) -> None:
    """Tests that all units in the application belong to the same replica set

    This test will be implemented by Raul as part of his onboarding task.

    Hint: multiple ways to do this, here are two but there are probably
    other ways to implement it:
    - check that each unit has the same replicaset name
    - create a URI for all units for the expected replicaset name and see if
      you can connect
    """
    pass


async def test_leader_is_primary_on_deployment(ops_test: OpsTest) -> None:
    """Tests that right after deployment that the primary unit is the leader
    """
    # grab leader unit
    leader_unit = None
    for unit_id in UNIT_IDS:
        unit = ops_test.model.applications[APP_NAME].units[unit_id]
        if await unit.is_leader_from_status():
            leader_unit = unit

    # verify that we have a leader
    assert leader_unit is not None, "No unit is leader"

    # connect to mongod
    connection = leader_unit.public_address + ":" + str(PORT)
    client = MongoClient(connection, replicaset="rs0")

    # verify primary status
    assert client.is_primary, "Leader is not primary"


# async def test_cluster_maintains_primary_after_deletion(ops_test: OpsTest) -> None:
#     # grab leader unit
#     leader_unit = None
#     for unit_id in UNIT_IDS:
#         unit = ops_test.model.applications[APP_NAME].units[unit_id]
#         if unit.is_leader_from_status():
#             leader_unit = unit
#
#     # verify that we have a leader
#     assert leader_unit is not None, "No unit is leader"
#
#     # destroy leader unit
#     ops_test.model.destroy_unit(leader_unit)
#
#     # count number of units that are the primary
#     number_of_primaries = 0
#     for unit_id in UNIT_IDS[:-1]:
#         # get unit
#         unit = ops_test.model.applications[APP_NAME].units[unit_id]
#
#         # connect to mongod
#         connection = unit.public_address + ":" + str(PORT)
#         client = MongoClient(connection, replicaset="rs0")
#
#         # check primary status
#         if client.is_primary:
#             number_of_primaries += 1
#
#     # check that exactly of the units is the leader
#     assert number_of_primaries == 1, (
#         "Expected one unit to be a primary: %s != 1" % (number_of_primaries)
#     )


def update_bind_ip(conf: str, ip_address: str) -> str:
    """ Updates mongod.conf contents to use the given ip address for bindIp

    Args:
        conf: contents of mongod.conf
        ip_address: ip adress of unit
    """
    mongo_config = yaml.safe_load(conf)
    mongo_config["net"]["bindIp"] = "localhost,{}".format(ip_address)
    return yaml.dump(mongo_config)
