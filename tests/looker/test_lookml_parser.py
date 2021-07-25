from metaphor.models.metadata_change_event import (
    DataPlatform,
    DatasetLogicalID,
    EntityType,
)

from metaphor.common.entity_id import EntityId
from metaphor.looker.lookml_parser import Connection, Explore, Model, parse_models

connection_map = {
    "snowflake": Connection(
        name="snowflake",
        platform=DataPlatform.SNOWFLAKE,
        database="db",
        default_schema="schema",
    )
}


def test_empty_model(test_root_dir):

    models_map = parse_models(test_root_dir + "/looker/empty_model", connection_map)

    expected = {"model1": Model(explores={})}
    assert models_map == expected


def test_basic(test_root_dir):

    models_map = parse_models(test_root_dir + "/looker/basic", connection_map)

    expected = {
        "model1": Model(
            explores={
                "explore1": Explore(
                    name="explore1",
                    description="description",
                    label="label",
                    upstream_datasets={
                        EntityId(
                            EntityType.DATASET,
                            DatasetLogicalID(
                                name="db.schema.view1", platform=DataPlatform.SNOWFLAKE
                            ),
                        )
                    },
                )
            }
        )
    }
    assert models_map == expected


def test_join(test_root_dir):

    models_map = parse_models(test_root_dir + "/looker/join", connection_map)

    expected = {
        "model1": Model(
            explores={
                "explore1": Explore(
                    name="explore1",
                    description="description",
                    label="label",
                    upstream_datasets={
                        EntityId(
                            EntityType.DATASET,
                            DatasetLogicalID(
                                name="db.schema.view1", platform=DataPlatform.SNOWFLAKE
                            ),
                        ),
                        EntityId(
                            EntityType.DATASET,
                            DatasetLogicalID(
                                name="db.schema2.view2", platform=DataPlatform.SNOWFLAKE
                            ),
                        ),
                    },
                )
            }
        )
    }
    assert models_map == expected


def test_explore_in_view(test_root_dir):

    models_map = parse_models(test_root_dir + "/looker/explore_in_view", connection_map)

    expected = {
        "model1": Model(
            explores={
                "explore1": Explore(
                    name="explore1",
                    description="description",
                    label="label",
                    upstream_datasets={
                        EntityId(
                            EntityType.DATASET,
                            DatasetLogicalID(
                                name="db.schema.view1", platform=DataPlatform.SNOWFLAKE
                            ),
                        )
                    },
                )
            }
        )
    }
    assert models_map == expected


def test_derived_table(test_root_dir):

    models_map = parse_models(test_root_dir + "/looker/derived_table", connection_map)

    expected = {
        "model": Model(
            explores={
                "explore1": Explore(
                    name="explore1",
                    description="description",
                    label="label",
                    upstream_datasets={
                        EntityId(
                            EntityType.DATASET,
                            DatasetLogicalID(
                                name="db.schema.table1", platform=DataPlatform.SNOWFLAKE
                            ),
                        )
                    },
                ),
                "explore2": Explore(
                    name="explore2",
                    description="description",
                    label="label",
                    upstream_datasets={
                        EntityId(
                            EntityType.DATASET,
                            DatasetLogicalID(
                                name="db.schema.table1", platform=DataPlatform.SNOWFLAKE
                            ),
                        )
                    },
                ),
            }
        )
    }
    assert models_map == expected
