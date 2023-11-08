from metaphor.common.event_util import EventUtil
from metaphor.models.metadata_change_event import (
    Dashboard,
    Dataset,
    Hierarchy,
    HierarchyLogicalID,
    KnowledgeCard,
    MetadataChangeEvent,
    Metric,
    Pipeline,
    QueryAttributions,
    QueryLogs,
    UserActivity,
    VirtualView,
)


def test_build_event():
    event_utils = EventUtil()

    assert event_utils.build_event(Dashboard()) == MetadataChangeEvent(
        dashboard=Dashboard()
    )
    assert event_utils.build_event(Dataset()) == MetadataChangeEvent(dataset=Dataset())
    assert event_utils.build_event(Hierarchy()) == MetadataChangeEvent(
        hierarchy=Hierarchy()
    )
    assert event_utils.build_event(KnowledgeCard()) == MetadataChangeEvent(
        knowledge_card=KnowledgeCard()
    )
    assert event_utils.build_event(Metric()) == MetadataChangeEvent(metric=Metric())
    assert event_utils.build_event(Pipeline()) == MetadataChangeEvent(
        pipeline=Pipeline()
    )
    assert event_utils.build_event(QueryAttributions()) == MetadataChangeEvent(
        query_attributions=QueryAttributions()
    )
    assert event_utils.build_event(QueryLogs()) == MetadataChangeEvent(
        query_logs=QueryLogs()
    )
    assert event_utils.build_event(VirtualView()) == MetadataChangeEvent(
        virtual_view=VirtualView()
    )
    assert event_utils.build_event(UserActivity()) == MetadataChangeEvent(
        user_activity=UserActivity()
    )


def test_trim_event():
    event_utils = EventUtil()

    assert event_utils.trim_event(Dashboard()) == {}
    assert event_utils.trim_event(Dataset()) == {}
    assert event_utils.trim_event(Hierarchy()) == {}
    assert event_utils.trim_event(KnowledgeCard()) == {}
    assert event_utils.trim_event(Metric()) == {}
    assert event_utils.trim_event(Pipeline()) == {}
    assert event_utils.trim_event(QueryLogs()) == {}
    assert event_utils.trim_event(UserActivity()) == {}
    assert event_utils.trim_event(VirtualView()) == {}

    assert event_utils.trim_event(
        Hierarchy(logical_id=HierarchyLogicalID(path=["a", "b"]))
    ) == {"logicalId": {"path": ["a", "b"]}}
