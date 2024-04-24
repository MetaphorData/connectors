from metaphor.common.entity_id import EntityId
from metaphor.looker.config import LookerConnectionConfig
from metaphor.looker.lookml_parser import Explore, Model, parse_project
from metaphor.models.metadata_change_event import (
    AssetStructure,
    DataPlatform,
    DatasetLogicalID,
    EntityType,
    EntityUpstream,
    LookerExplore,
    LookerExploreJoin,
    LookerView,
    LookerViewDimension,
    LookerViewMeasure,
    LookerViewQuery,
    SystemTag,
    SystemTags,
    SystemTagSource,
    VirtualView,
    VirtualViewLogicalID,
    VirtualViewType,
)
from tests.test_utils import compare_list_ignore_order

connection_map = {
    "snowflake": LookerConnectionConfig(
        database="db",
        default_schema="schema",
        platform=DataPlatform.SNOWFLAKE,
        account="account",
    ),
    "bigquery": LookerConnectionConfig(
        database="db",
        default_schema="schema",
        platform=DataPlatform.BIGQUERY,
    ),
}


def test_empty_model(test_root_dir):
    models_map, virtual_views = parse_project(
        f"{test_root_dir}/looker/empty_model", connection_map
    )

    expected = {"model1": Model(explores={})}
    assert models_map == expected
    assert virtual_views == []


def test_basic(test_root_dir):
    models_map, virtual_views = parse_project(
        f"{test_root_dir}/looker/basic", connection_map, "http://foo/files"
    )

    dataset_id = EntityId(
        EntityType.DATASET,
        DatasetLogicalID(
            name="db.schema.view1",
            platform=DataPlatform.SNOWFLAKE,
            account="account",
        ),
    )

    virtual_view_id = EntityId(
        EntityType.VIRTUAL_VIEW,
        VirtualViewLogicalID(name="model1.view1", type=VirtualViewType.LOOKER_VIEW),
    )

    assert models_map == {
        "model1": Model(
            explores={
                "explore1": Explore(
                    name="explore1",
                )
            }
        )
    }

    assert virtual_views == [
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model1.view1", type=VirtualViewType.LOOKER_VIEW
            ),
            looker_view=LookerView(
                dimensions=[LookerViewDimension(data_type="string", field="country")],
                measures=[
                    LookerViewMeasure(field="average_measurement", type="average")
                ],
                source_datasets=[str(dataset_id)],
                url="http://foo/files/view1.view.lkml",
            ),
            entity_upstream=EntityUpstream(source_entities=[str(dataset_id)]),
            structure=AssetStructure(
                directories=["model1"],
                name="view1",
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model1.explore1", type=VirtualViewType.LOOKER_EXPLORE
            ),
            looker_explore=LookerExplore(
                model_name="model1",
                base_view=str(virtual_view_id),
                description="description",
                label="label",
                url="http://foo/files/model1.model.lkml",
            ),
            structure=AssetStructure(
                directories=["model1"],
                name="explore1",
            ),
            entity_upstream=EntityUpstream(
                source_entities=[str(virtual_view_id)],
            ),
            system_tags=SystemTags(
                tags=[
                    SystemTag(
                        system_tag_source=SystemTagSource.LOOKER,
                        value="tag1",
                    ),
                    SystemTag(
                        system_tag_source=SystemTagSource.LOOKER,
                        value="tag2",
                    ),
                ]
            ),
        ),
    ]


def test_join(test_root_dir):
    models_map, virtual_views = parse_project(
        f"{test_root_dir}/looker/join", connection_map, ""
    )

    dataset_id1 = EntityId(
        EntityType.DATASET,
        DatasetLogicalID(
            name="db.schema.view1",
            platform=DataPlatform.SNOWFLAKE,
            account="account",
        ),
    )

    dataset_id2 = EntityId(
        EntityType.DATASET,
        DatasetLogicalID(
            name="db.schema2.view2",
            platform=DataPlatform.SNOWFLAKE,
            account="account",
        ),
    )

    virtual_view_id1 = EntityId(
        EntityType.VIRTUAL_VIEW,
        VirtualViewLogicalID(name="model1.view1", type=VirtualViewType.LOOKER_VIEW),
    )

    virtual_view_id2 = EntityId(
        EntityType.VIRTUAL_VIEW,
        VirtualViewLogicalID(name="model1.view2", type=VirtualViewType.LOOKER_VIEW),
    )

    expected = {
        "model1": Model(
            explores={
                "explore1": Explore(
                    name="explore1",
                )
            }
        )
    }
    assert models_map == expected

    assert compare_list_ignore_order(
        virtual_views,
        [
            VirtualView(
                logical_id=VirtualViewLogicalID(
                    name="model1.view1", type=VirtualViewType.LOOKER_VIEW
                ),
                looker_view=LookerView(
                    dimensions=[
                        LookerViewDimension(data_type="string", field="country")
                    ],
                    measures=[
                        LookerViewMeasure(field="average_measurement", type="average")
                    ],
                    source_datasets=[str(dataset_id1)],
                ),
                entity_upstream=EntityUpstream(source_entities=[str(dataset_id1)]),
                structure=AssetStructure(
                    directories=["model1"],
                    name="view1",
                ),
            ),
            VirtualView(
                logical_id=VirtualViewLogicalID(
                    name="model1.view2", type=VirtualViewType.LOOKER_VIEW
                ),
                looker_view=LookerView(
                    dimensions=[
                        LookerViewDimension(data_type="string", field="country")
                    ],
                    measures=[
                        LookerViewMeasure(field="average_measurement", type="average")
                    ],
                    source_datasets=[str(dataset_id2)],
                ),
                entity_upstream=EntityUpstream(source_entities=[str(dataset_id2)]),
                structure=AssetStructure(
                    directories=["model1"],
                    name="view2",
                ),
            ),
            VirtualView(
                logical_id=VirtualViewLogicalID(
                    name="model1.explore1", type=VirtualViewType.LOOKER_EXPLORE
                ),
                looker_explore=LookerExplore(
                    model_name="model1",
                    base_view=str(virtual_view_id1),
                    description="description",
                    joins=[
                        LookerExploreJoin(
                            on_clause="${view2.country} = ${view1.country}",
                            relationship="many_to_one",
                            type="left_outer",
                            view=str(virtual_view_id2),
                        ),
                        LookerExploreJoin(
                            on_clause="${view2.country} = ${view1.country}",
                            relationship="one_to_one",
                            type="left_outer",
                            view=str(virtual_view_id1),
                        ),
                    ],
                    label="label",
                ),
                structure=AssetStructure(
                    directories=["model1"],
                    name="explore1",
                ),
                entity_upstream=EntityUpstream(
                    source_entities=[str(virtual_view_id1), str(virtual_view_id2)]
                ),
            ),
        ],
    )


def test_explore_in_view(test_root_dir):
    models_map, virtual_views = parse_project(
        f"{test_root_dir}/looker/explore_in_view", connection_map
    )

    dataset_id = EntityId(
        EntityType.DATASET,
        DatasetLogicalID(
            name="db.schema.view1",
            platform=DataPlatform.SNOWFLAKE,
            account="account",
        ),
    )

    virtual_view_id = EntityId(
        EntityType.VIRTUAL_VIEW,
        VirtualViewLogicalID(name="model1.view1", type=VirtualViewType.LOOKER_VIEW),
    )

    expected = {
        "model1": Model(
            explores={
                "explore1": Explore(
                    name="explore1",
                )
            }
        )
    }
    assert models_map == expected

    assert compare_list_ignore_order(
        virtual_views,
        [
            VirtualView(
                logical_id=VirtualViewLogicalID(
                    name="model1.view1", type=VirtualViewType.LOOKER_VIEW
                ),
                looker_view=LookerView(
                    dimensions=[
                        LookerViewDimension(data_type="string", field="country")
                    ],
                    measures=[
                        LookerViewMeasure(field="average_measurement", type="average")
                    ],
                    source_datasets=[str(dataset_id)],
                ),
                entity_upstream=EntityUpstream(source_entities=[str(dataset_id)]),
                structure=AssetStructure(
                    directories=["model1"],
                    name="view1",
                ),
            ),
            VirtualView(
                logical_id=VirtualViewLogicalID(
                    name="model1.explore1", type=VirtualViewType.LOOKER_EXPLORE
                ),
                looker_explore=LookerExplore(
                    model_name="model1",
                    base_view=str(virtual_view_id),
                    description="description",
                    label="label",
                ),
                structure=AssetStructure(
                    directories=["model1"],
                    name="explore1",
                ),
                entity_upstream=EntityUpstream(source_entities=[str(virtual_view_id)]),
            ),
        ],
    )


def test_derived_table(test_root_dir):
    models_map, virtual_views = parse_project(
        f"{test_root_dir}/looker/derived_table", connection_map
    )

    virtual_view_id1 = EntityId(
        EntityType.VIRTUAL_VIEW,
        VirtualViewLogicalID(name="model.view1", type=VirtualViewType.LOOKER_VIEW),
    )

    virtual_view_id2 = EntityId(
        EntityType.VIRTUAL_VIEW,
        VirtualViewLogicalID(name="model.view2", type=VirtualViewType.LOOKER_VIEW),
    )

    virtual_view_id3 = EntityId(
        EntityType.VIRTUAL_VIEW,
        VirtualViewLogicalID(name="model.view3", type=VirtualViewType.LOOKER_VIEW),
    )

    expected = {
        "model": Model(
            explores={
                "explore1": Explore(name="explore1"),
                "explore2": Explore(name="explore2"),
                "explore3": Explore(name="explore3"),
            }
        )
    }
    assert models_map == expected

    assert virtual_views == [
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.view1", type=VirtualViewType.LOOKER_VIEW
            ),
            structure=AssetStructure(
                directories=["model"],
                name="view1",
            ),
            looker_view=LookerView(
                query=LookerViewQuery(
                    default_database="db",
                    default_schema="schema",
                    query="SELECT * FROM table1",
                    source_dataset_account="account",
                    source_platform=DataPlatform.SNOWFLAKE,
                )
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.view2", type=VirtualViewType.LOOKER_VIEW
            ),
            structure=AssetStructure(
                directories=["model"],
                name="view2",
            ),
            looker_view=LookerView(
                query=LookerViewQuery(
                    default_database="db",
                    default_schema="schema",
                    query="SELECT * FROM ${view1.SQL_TABLE_NAME}",
                    source_dataset_account="account",
                    source_platform=DataPlatform.SNOWFLAKE,
                ),
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.view3", type=VirtualViewType.LOOKER_VIEW
            ),
            structure=AssetStructure(
                directories=["model"],
                name="view3",
            ),
            looker_view=LookerView(),
            entity_upstream=EntityUpstream(source_entities=[str(virtual_view_id1)]),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.explore1", type=VirtualViewType.LOOKER_EXPLORE
            ),
            looker_explore=LookerExplore(
                model_name="model",
                base_view=str(virtual_view_id1),
            ),
            structure=AssetStructure(
                directories=["model"],
                name="explore1",
            ),
            entity_upstream=EntityUpstream(source_entities=[str(virtual_view_id1)]),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.explore2", type=VirtualViewType.LOOKER_EXPLORE
            ),
            looker_explore=LookerExplore(
                model_name="model",
                base_view=str(virtual_view_id2),
            ),
            structure=AssetStructure(
                directories=["model"],
                name="explore2",
            ),
            entity_upstream=EntityUpstream(source_entities=[str(virtual_view_id2)]),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.explore3", type=VirtualViewType.LOOKER_EXPLORE
            ),
            looker_explore=LookerExplore(
                model_name="model",
                base_view=str(virtual_view_id3),
            ),
            structure=AssetStructure(
                directories=["model"],
                name="explore3",
            ),
            entity_upstream=EntityUpstream(source_entities=[str(virtual_view_id3)]),
        ),
    ]


def test_sql_table_name(test_root_dir):
    models_map, virtual_views = parse_project(
        f"{test_root_dir}/looker/sql_table_name", connection_map
    )

    virtual_view_id1 = EntityId(
        EntityType.VIRTUAL_VIEW,
        VirtualViewLogicalID(name="model.view1", type=VirtualViewType.LOOKER_VIEW),
    )

    dataset_id1 = EntityId(
        EntityType.DATASET,
        DatasetLogicalID(
            name="db.schema1.table1",
            platform=DataPlatform.BIGQUERY,
        ),
    )

    assert models_map == {
        "model": Model(
            explores={
                "explore1": Explore(
                    name="explore1",
                ),
            }
        )
    }

    assert virtual_views == [
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.view1", type=VirtualViewType.LOOKER_VIEW
            ),
            looker_view=LookerView(
                source_datasets=[str(dataset_id1)],
            ),
            entity_upstream=EntityUpstream(source_entities=[str(dataset_id1)]),
            structure=AssetStructure(
                directories=["model"],
                name="view1",
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.explore1", type=VirtualViewType.LOOKER_EXPLORE
            ),
            looker_explore=LookerExplore(
                model_name="model",
                base_view=str(virtual_view_id1),
            ),
            structure=AssetStructure(
                directories=["model"],
                name="explore1",
            ),
            entity_upstream=EntityUpstream(source_entities=[str(virtual_view_id1)]),
        ),
    ]


def test_include_relative_to_model(test_root_dir):
    _, virtual_views = parse_project(
        f"{test_root_dir}/looker/include_relative_to_model", connection_map
    )

    dataset_id1 = EntityId(
        EntityType.DATASET,
        DatasetLogicalID(
            name="db.schema.view1",
            platform=DataPlatform.BIGQUERY,
        ),
    )

    dataset_id2 = EntityId(
        EntityType.DATASET,
        DatasetLogicalID(
            name="db.schema.view2",
            platform=DataPlatform.BIGQUERY,
        ),
    )

    assert virtual_views == [
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.view1", type=VirtualViewType.LOOKER_VIEW
            ),
            looker_view=LookerView(
                source_datasets=[str(dataset_id1)],
            ),
            entity_upstream=EntityUpstream(source_entities=[str(dataset_id1)]),
            structure=AssetStructure(
                directories=["model"],
                name="view1",
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.view2", type=VirtualViewType.LOOKER_VIEW
            ),
            looker_view=LookerView(
                source_datasets=[str(dataset_id2)],
            ),
            entity_upstream=EntityUpstream(source_entities=[str(dataset_id2)]),
            structure=AssetStructure(
                directories=["model"],
                name="view2",
            ),
        ),
    ]


def test_complex_includes(test_root_dir):
    models_map, virtual_views = parse_project(
        f"{test_root_dir}/looker/complex_includes", connection_map
    )

    virtual_view_id1 = EntityId(
        EntityType.VIRTUAL_VIEW,
        VirtualViewLogicalID(name="model.view1", type=VirtualViewType.LOOKER_VIEW),
    )

    dataset_id1 = EntityId(
        EntityType.DATASET,
        DatasetLogicalID(
            name="db.schema.view1",
            platform=DataPlatform.BIGQUERY,
        ),
    )

    dataset_id2 = EntityId(
        EntityType.DATASET,
        DatasetLogicalID(
            name="db.schema.view2",
            platform=DataPlatform.BIGQUERY,
        ),
    )

    dataset_id3 = EntityId(
        EntityType.DATASET,
        DatasetLogicalID(
            name="db.schema.view3",
            platform=DataPlatform.BIGQUERY,
        ),
    )

    dataset_id4 = EntityId(
        EntityType.DATASET,
        DatasetLogicalID(
            name="db.schema.view4",
            platform=DataPlatform.BIGQUERY,
        ),
    )

    assert models_map == {
        "model": Model(
            explores={
                "explore1": Explore(
                    name="explore1",
                ),
            }
        )
    }

    assert virtual_views == [
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.view1", type=VirtualViewType.LOOKER_VIEW
            ),
            looker_view=LookerView(
                source_datasets=[str(dataset_id1)],
            ),
            entity_upstream=EntityUpstream(source_entities=[str(dataset_id1)]),
            structure=AssetStructure(
                directories=["model"],
                name="view1",
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.view2", type=VirtualViewType.LOOKER_VIEW
            ),
            looker_view=LookerView(
                source_datasets=[str(dataset_id2)],
            ),
            entity_upstream=EntityUpstream(source_entities=[str(dataset_id2)]),
            structure=AssetStructure(
                directories=["model"],
                name="view2",
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.view3", type=VirtualViewType.LOOKER_VIEW
            ),
            looker_view=LookerView(
                source_datasets=[str(dataset_id3)],
            ),
            entity_upstream=EntityUpstream(source_entities=[str(dataset_id3)]),
            structure=AssetStructure(
                directories=["model"],
                name="view3",
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.view4", type=VirtualViewType.LOOKER_VIEW
            ),
            looker_view=LookerView(
                source_datasets=[str(dataset_id4)],
            ),
            entity_upstream=EntityUpstream(source_entities=[str(dataset_id4)]),
            structure=AssetStructure(
                directories=["model"],
                name="view4",
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.explore1", type=VirtualViewType.LOOKER_EXPLORE
            ),
            looker_explore=LookerExplore(
                model_name="model",
                base_view=str(virtual_view_id1),
            ),
            structure=AssetStructure(
                directories=["model"],
                name="explore1",
            ),
            entity_upstream=EntityUpstream(source_entities=[str(virtual_view_id1)]),
        ),
    ]


def test_view_extension(test_root_dir):
    models_map, virtual_views = parse_project(
        f"{test_root_dir}/looker/view_extension", connection_map
    )

    virtual_view_id1 = EntityId(
        EntityType.VIRTUAL_VIEW,
        VirtualViewLogicalID(name="model.view1", type=VirtualViewType.LOOKER_VIEW),
    )

    dataset_table1 = EntityId(
        EntityType.DATASET,
        DatasetLogicalID(
            name="db.schema.table1",
            platform=DataPlatform.BIGQUERY,
        ),
    )

    dataset_table3 = EntityId(
        EntityType.DATASET,
        DatasetLogicalID(
            name="db.schema.table3",
            platform=DataPlatform.BIGQUERY,
        ),
    )

    dataset_base_view3 = EntityId(
        EntityType.DATASET,
        DatasetLogicalID(
            name="db.schema.base_view3",
            platform=DataPlatform.BIGQUERY,
        ),
    )

    assert models_map == {
        "model": Model(
            explores={
                "explore1": Explore(name="explore1"),
            }
        )
    }

    assert virtual_views == [
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.view1", type=VirtualViewType.LOOKER_VIEW
            ),
            structure=AssetStructure(
                directories=["model"],
                name="view1",
            ),
            looker_view=LookerView(
                source_datasets=[str(dataset_table1)],
            ),
            entity_upstream=EntityUpstream(source_entities=[str(dataset_table1)]),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.view2", type=VirtualViewType.LOOKER_VIEW
            ),
            structure=AssetStructure(
                directories=["model"],
                name="view2",
            ),
            looker_view=LookerView(
                query=LookerViewQuery(
                    default_database="db",
                    default_schema="schema",
                    query="SELECT * FROM table2",
                    source_platform=DataPlatform.BIGQUERY,
                )
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.view3", type=VirtualViewType.LOOKER_VIEW
            ),
            structure=AssetStructure(
                directories=["model"],
                name="view3",
            ),
            looker_view=LookerView(
                query=LookerViewQuery(
                    default_database="db",
                    default_schema="schema",
                    query="SELECT * FROM table2",
                    source_platform=DataPlatform.BIGQUERY,
                )
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.view4", type=VirtualViewType.LOOKER_VIEW
            ),
            looker_view=LookerView(
                source_datasets=[str(dataset_table3)],
            ),
            entity_upstream=EntityUpstream(source_entities=[str(dataset_table3)]),
            structure=AssetStructure(
                directories=["model"],
                name="view4",
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.base_view2", type=VirtualViewType.LOOKER_VIEW
            ),
            structure=AssetStructure(
                directories=["model"],
                name="base_view2",
            ),
            looker_view=LookerView(
                query=LookerViewQuery(
                    default_database="db",
                    default_schema="schema",
                    query="SELECT * FROM table2",
                    source_platform=DataPlatform.BIGQUERY,
                )
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.base_view3", type=VirtualViewType.LOOKER_VIEW
            ),
            structure=AssetStructure(
                directories=["model"],
                name="base_view3",
            ),
            looker_view=LookerView(
                source_datasets=[str(dataset_base_view3)],
            ),
            entity_upstream=EntityUpstream(source_entities=[str(dataset_base_view3)]),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.explore1", type=VirtualViewType.LOOKER_EXPLORE
            ),
            structure=AssetStructure(
                directories=["model"],
                name="explore1",
            ),
            looker_explore=LookerExplore(
                model_name="model",
                base_view=str(virtual_view_id1),
            ),
            entity_upstream=EntityUpstream(source_entities=[str(virtual_view_id1)]),
        ),
    ]


def test_explore_extension(test_root_dir):
    models_map, virtual_views = parse_project(
        f"{test_root_dir}/looker/explore_extension", connection_map
    )

    virtual_view1 = EntityId(
        EntityType.VIRTUAL_VIEW,
        VirtualViewLogicalID(name="model.view1", type=VirtualViewType.LOOKER_VIEW),
    )

    virtual_view2 = EntityId(
        EntityType.VIRTUAL_VIEW,
        VirtualViewLogicalID(name="model.view2", type=VirtualViewType.LOOKER_VIEW),
    )

    virtual_view3 = EntityId(
        EntityType.VIRTUAL_VIEW,
        VirtualViewLogicalID(name="model.view3", type=VirtualViewType.LOOKER_VIEW),
    )

    dataset_view1 = EntityId(
        EntityType.DATASET,
        DatasetLogicalID(
            name="db.schema.view1",
            platform=DataPlatform.BIGQUERY,
        ),
    )

    dataset_view2 = EntityId(
        EntityType.DATASET,
        DatasetLogicalID(
            name="db.schema.view2",
            platform=DataPlatform.BIGQUERY,
        ),
    )

    dataset_view3 = EntityId(
        EntityType.DATASET,
        DatasetLogicalID(
            name="db.schema.view3",
            platform=DataPlatform.BIGQUERY,
        ),
    )

    assert models_map == {
        "model": Model(
            explores={
                "explore1": Explore(name="explore1"),
                "explore2": Explore(name="explore2"),
                "explore3": Explore(name="explore3"),
                "explore4": Explore(name="explore4"),
                "base_explore1": Explore(name="base_explore1"),
                "base_explore2": Explore(name="base_explore2"),
                "view3": Explore(name="view3"),
            }
        )
    }

    assert virtual_views == [
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.view1", type=VirtualViewType.LOOKER_VIEW
            ),
            looker_view=LookerView(
                source_datasets=[str(dataset_view1)],
            ),
            entity_upstream=EntityUpstream(source_entities=[str(dataset_view1)]),
            structure=AssetStructure(
                directories=["model"],
                name="view1",
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.view2", type=VirtualViewType.LOOKER_VIEW
            ),
            looker_view=LookerView(
                source_datasets=[str(dataset_view2)],
            ),
            entity_upstream=EntityUpstream(source_entities=[str(dataset_view2)]),
            structure=AssetStructure(
                directories=["model"],
                name="view2",
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.view3", type=VirtualViewType.LOOKER_VIEW
            ),
            looker_view=LookerView(
                source_datasets=[str(dataset_view3)],
            ),
            entity_upstream=EntityUpstream(source_entities=[str(dataset_view3)]),
            structure=AssetStructure(
                directories=["model"],
                name="view3",
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.explore1", type=VirtualViewType.LOOKER_EXPLORE
            ),
            looker_explore=LookerExplore(
                model_name="model",
                base_view=str(virtual_view1),
            ),
            structure=AssetStructure(
                directories=["model"],
                name="explore1",
            ),
            entity_upstream=EntityUpstream(source_entities=[str(virtual_view1)]),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.explore2", type=VirtualViewType.LOOKER_EXPLORE
            ),
            looker_explore=LookerExplore(
                model_name="model",
                base_view=str(virtual_view2),
            ),
            structure=AssetStructure(
                directories=["model"],
                name="explore2",
            ),
            entity_upstream=EntityUpstream(source_entities=[str(virtual_view2)]),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.explore3", type=VirtualViewType.LOOKER_EXPLORE
            ),
            looker_explore=LookerExplore(
                model_name="model",
                base_view=str(virtual_view1),
            ),
            structure=AssetStructure(
                directories=["model"],
                name="explore3",
            ),
            entity_upstream=EntityUpstream(source_entities=[str(virtual_view1)]),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.explore4", type=VirtualViewType.LOOKER_EXPLORE
            ),
            looker_explore=LookerExplore(
                model_name="model",
                base_view=str(virtual_view3),
            ),
            structure=AssetStructure(
                directories=["model"],
                name="explore4",
            ),
            entity_upstream=EntityUpstream(source_entities=[str(virtual_view3)]),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.base_explore2", type=VirtualViewType.LOOKER_EXPLORE
            ),
            looker_explore=LookerExplore(
                model_name="model",
                base_view=str(virtual_view2),
            ),
            structure=AssetStructure(
                directories=["model"],
                name="base_explore2",
            ),
            entity_upstream=EntityUpstream(source_entities=[str(virtual_view2)]),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.view3", type=VirtualViewType.LOOKER_EXPLORE
            ),
            looker_explore=LookerExplore(
                model_name="model",
                base_view=str(virtual_view3),
            ),
            structure=AssetStructure(
                directories=["model"],
                name="view3",
            ),
            entity_upstream=EntityUpstream(source_entities=[str(virtual_view3)]),
        ),
    ]
