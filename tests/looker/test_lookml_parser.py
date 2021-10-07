from metaphor.models.metadata_change_event import (
    DataPlatform,
    DatasetLogicalID,
    EntityType,
    LookerExplore,
    LookerExploreJoin,
    LookerView,
    LookerViewDimension,
    LookerViewMeasure,
    VirtualView,
    VirtualViewLogicalID,
    VirtualViewType,
)

from metaphor.common.entity_id import EntityId
from metaphor.looker.lookml_parser import Connection, Explore, Model, parse_project

connection_map = {
    "snowflake": Connection(
        name="snowflake",
        platform=DataPlatform.SNOWFLAKE,
        database="db",
        account="account",
        default_schema="schema",
    )
}


def test_empty_model(test_root_dir):
    models_map, virtual_views = parse_project(
        test_root_dir + "/looker/empty_model", connection_map
    )

    expected = {"model1": Model(explores={})}
    assert models_map == expected
    assert virtual_views == []


def test_basic(test_root_dir):
    models_map, virtual_views = parse_project(
        test_root_dir + "/looker/basic", connection_map
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
                    description="description",
                    label="label",
                    upstream_datasets={dataset_id},
                )
            }
        )
    }
    assert models_map == expected

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
        ),
    ]


def test_join(test_root_dir):
    models_map, virtual_views = parse_project(
        test_root_dir + "/looker/join", connection_map
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
                    description="description",
                    label="label",
                    upstream_datasets={dataset_id1, dataset_id2},
                )
            }
        )
    }
    assert models_map == expected

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
                source_datasets=[str(dataset_id1)],
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model1.view2", type=VirtualViewType.LOOKER_VIEW
            ),
            looker_view=LookerView(
                dimensions=[LookerViewDimension(data_type="string", field="country")],
                measures=[
                    LookerViewMeasure(field="average_measurement", type="average")
                ],
                source_datasets=[str(dataset_id2)],
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
        ),
    ]


def test_explore_in_view(test_root_dir):
    models_map, virtual_views = parse_project(
        test_root_dir + "/looker/explore_in_view", connection_map
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
                    description="description",
                    label="label",
                    upstream_datasets={dataset_id},
                )
            }
        )
    }
    assert models_map == expected

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
        ),
    ]


def test_derived_table(test_root_dir):
    models_map, virtual_views = parse_project(
        test_root_dir + "/looker/derived_table", connection_map
    )

    dataset_id = EntityId(
        EntityType.DATASET,
        DatasetLogicalID(
            name="db.schema.table1",
            platform=DataPlatform.SNOWFLAKE,
            account="account",
        ),
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
                "explore1": Explore(
                    name="explore1",
                    upstream_datasets={
                        EntityId(
                            type=EntityType.DATASET,
                            logicalId=DatasetLogicalID(
                                account="account",
                                name="db.schema.table1",
                                platform=DataPlatform.SNOWFLAKE,
                            ),
                        )
                    },
                    description="description",
                    label="label",
                ),
                "explore2": Explore(
                    name="explore2",
                    upstream_datasets={
                        EntityId(
                            type=EntityType.DATASET,
                            logicalId=DatasetLogicalID(
                                account="account",
                                name="db.schema.table1",
                                platform=DataPlatform.SNOWFLAKE,
                            ),
                        )
                    },
                    description="description",
                    label="label",
                ),
                "explore3": Explore(
                    name="explore3",
                    upstream_datasets={
                        EntityId(
                            type=EntityType.DATASET,
                            logicalId=DatasetLogicalID(
                                account="account",
                                name="db.schema.table1",
                                platform=DataPlatform.SNOWFLAKE,
                            ),
                        )
                    },
                    description="description3",
                    label="label3",
                ),
            }
        )
    }
    assert models_map == expected

    assert virtual_views == [
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.view1", type=VirtualViewType.LOOKER_VIEW
            ),
            looker_view=LookerView(
                dimensions=[LookerViewDimension(data_type="string", field="country")],
                measures=[
                    LookerViewMeasure(field="average_measurement", type="average")
                ],
                source_datasets=[str(dataset_id)],
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.view2", type=VirtualViewType.LOOKER_VIEW
            ),
            looker_view=LookerView(
                dimensions=[LookerViewDimension(data_type="string", field="country")],
                measures=[
                    LookerViewMeasure(field="average_measurement", type="average")
                ],
                source_datasets=[str(dataset_id)],
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.view3", type=VirtualViewType.LOOKER_VIEW
            ),
            looker_view=LookerView(
                dimensions=[LookerViewDimension(data_type="string", field="country")],
                source_datasets=["DATASET~22F73B93BC1BBDE2A552F0B23A83626B"],
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.explore1", type=VirtualViewType.LOOKER_EXPLORE
            ),
            looker_explore=LookerExplore(
                model_name="model",
                base_view=str(virtual_view_id1),
                description="description",
                label="label",
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.explore2", type=VirtualViewType.LOOKER_EXPLORE
            ),
            looker_explore=LookerExplore(
                model_name="model",
                base_view=str(virtual_view_id2),
                description="description",
                label="label",
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="model.explore3", type=VirtualViewType.LOOKER_EXPLORE
            ),
            looker_explore=LookerExplore(
                model_name="model",
                base_view=str(virtual_view_id3),
                description="description3",
                label="label3",
            ),
        ),
    ]
