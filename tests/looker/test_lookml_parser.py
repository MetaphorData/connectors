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
from metaphor.looker.lookml_parser import Connection, Explore, Model, parse_models

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
    models_map, virtual_views = parse_models(
        test_root_dir + "/looker/empty_model", connection_map
    )

    expected = {"model1": Model(explores={})}
    assert models_map == expected
    assert virtual_views == []


def test_basic(test_root_dir):
    models_map, virtual_views = parse_models(
        test_root_dir + "/looker/basic", connection_map
    )

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
                                name="db.schema.view1",
                                platform=DataPlatform.SNOWFLAKE,
                                account="account",
                            ),
                        )
                    },
                )
            }
        )
    }
    assert models_map == expected

    assert virtual_views == [
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="view1", type=VirtualViewType.LOOKER_VIEW
            ),
            looker_view=LookerView(
                dimensions=[LookerViewDimension(data_type="string", field="country")],
                measures=[
                    LookerViewMeasure(field="average_measurement", type="average")
                ],
                source_dataset="DATASET~5881AB4C0A42EF1C15F6C02C0D14AD43",
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="explore1", type=VirtualViewType.LOOKER_EXPLORE
            ),
            looker_explore=LookerExplore(
                base_view="VIRTUAL_VIEW~361092A8CFF1EFEFD695B221B21943B3",
                description="description",
                label="label",
            ),
        ),
    ]


def test_join(test_root_dir):
    models_map, virtual_views = parse_models(
        test_root_dir + "/looker/join", connection_map
    )

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
                                name="db.schema.view1",
                                platform=DataPlatform.SNOWFLAKE,
                                account="account",
                            ),
                        ),
                        EntityId(
                            EntityType.DATASET,
                            DatasetLogicalID(
                                name="db.schema2.view2",
                                platform=DataPlatform.SNOWFLAKE,
                                account="account",
                            ),
                        ),
                    },
                )
            }
        )
    }
    assert models_map == expected

    assert virtual_views == [
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="view1", type=VirtualViewType.LOOKER_VIEW
            ),
            looker_view=LookerView(
                dimensions=[LookerViewDimension(data_type="string", field="country")],
                measures=[
                    LookerViewMeasure(field="average_measurement", type="average")
                ],
                source_dataset="DATASET~5881AB4C0A42EF1C15F6C02C0D14AD43",
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="view2", type=VirtualViewType.LOOKER_VIEW
            ),
            looker_view=LookerView(
                dimensions=[LookerViewDimension(data_type="string", field="country")],
                measures=[
                    LookerViewMeasure(field="average_measurement", type="average")
                ],
                source_dataset="DATASET~ED9F33ADDE4537C4DE68E6BD18A3899B",
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="explore1", type=VirtualViewType.LOOKER_EXPLORE
            ),
            looker_explore=LookerExplore(
                base_view="VIRTUAL_VIEW~361092A8CFF1EFEFD695B221B21943B3",
                description="description",
                joins=[
                    LookerExploreJoin(
                        on_clause="${view2.country} = ${view1.country}",
                        relationship="one_to_one",
                        type="left_outer",
                        view="VIRTUAL_VIEW~3E7FDDC06A074F60ED05B95E04423CDA",
                    ),
                    LookerExploreJoin(
                        on_clause="${view2.country} = ${view1.country}",
                        relationship="one_to_one",
                        type="left_outer",
                        view="VIRTUAL_VIEW~361092A8CFF1EFEFD695B221B21943B3",
                    ),
                ],
                label="label",
            ),
        ),
    ]


def test_explore_in_view(test_root_dir):
    models_map, virtual_views = parse_models(
        test_root_dir + "/looker/explore_in_view", connection_map
    )

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
                                name="db.schema.view1",
                                platform=DataPlatform.SNOWFLAKE,
                                account="account",
                            ),
                        )
                    },
                )
            }
        )
    }
    assert models_map == expected

    assert virtual_views == [
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="view1", type=VirtualViewType.LOOKER_VIEW
            ),
            looker_view=LookerView(
                dimensions=[LookerViewDimension(data_type="string", field="country")],
                measures=[
                    LookerViewMeasure(field="average_measurement", type="average")
                ],
                source_dataset="DATASET~5881AB4C0A42EF1C15F6C02C0D14AD43",
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="explore1", type=VirtualViewType.LOOKER_EXPLORE
            ),
            looker_explore=LookerExplore(
                base_view="VIRTUAL_VIEW~361092A8CFF1EFEFD695B221B21943B3",
                description="description",
                label="label",
            ),
        ),
    ]


def test_derived_table(test_root_dir):
    models_map, virtual_views = parse_models(
        test_root_dir + "/looker/derived_table", connection_map
    )

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
                                name="db.schema.table1",
                                platform=DataPlatform.SNOWFLAKE,
                                account="account",
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
                                name="db.schema.table1",
                                platform=DataPlatform.SNOWFLAKE,
                                account="account",
                            ),
                        )
                    },
                ),
            }
        )
    }
    assert models_map == expected

    assert virtual_views == [
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="view1", type=VirtualViewType.LOOKER_VIEW
            ),
            looker_view=LookerView(
                dimensions=[LookerViewDimension(data_type="string", field="country")],
                measures=[
                    LookerViewMeasure(field="average_measurement", type="average")
                ],
                source_dataset="DATASET~5881AB4C0A42EF1C15F6C02C0D14AD43",
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="view2", type=VirtualViewType.LOOKER_VIEW
            ),
            looker_view=LookerView(
                dimensions=[LookerViewDimension(data_type="string", field="country")],
                measures=[
                    LookerViewMeasure(field="average_measurement", type="average")
                ],
                source_dataset="DATASET~66B2CABC0E5FA838E7A279EA377DCD7E",
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="explore1", type=VirtualViewType.LOOKER_EXPLORE
            ),
            looker_explore=LookerExplore(
                base_view="VIRTUAL_VIEW~361092A8CFF1EFEFD695B221B21943B3",
                description="description",
                label="label",
            ),
        ),
        VirtualView(
            logical_id=VirtualViewLogicalID(
                name="explore2", type=VirtualViewType.LOOKER_EXPLORE
            ),
            looker_explore=LookerExplore(
                base_view="VIRTUAL_VIEW~3E7FDDC06A074F60ED05B95E04423CDA",
                description="description",
                label="label",
            ),
        ),
    ]
