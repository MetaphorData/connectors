from metaphor.looker.folder import FolderMetadata, _build_hierarchies, build_directories


def test_build_directories(test_root_dir) -> None:

    folder_map = {
        "1": FolderMetadata(id="1", name="folder1", parent_id=None),
        "2": FolderMetadata(id="2", name="folder2", parent_id="1"),
        "3": FolderMetadata(id="3", name="folder3", parent_id="2"),
    }

    folders: dict = {}

    assert build_directories("1", folder_map, folders) == ["1"]
    assert build_directories("2", folder_map, folders) == ["1", "2"]
    assert build_directories("3", folder_map, folders) == ["1", "2", "3"]
    assert len(folders) == 3


def test_build_hierarchy(test_root_dir) -> None:

    folder_map = {
        "1": FolderMetadata(id="1", name="folder1", parent_id=None),
        "2": FolderMetadata(id="2", name="folder2", parent_id="1"),
        "3": FolderMetadata(id="3", name="folder3", parent_id="2"),
    }

    folders: dict = {}

    _build_hierarchies(["1", "2", "3"], folder_map, folders)
    assert len(folders) == 3
    assert folders["1"].hierarchy_info.name == "folder1"
    assert folders["1"].logical_id.path == ["LOOKER", "1"]
    assert folders["2"].hierarchy_info.name == "folder2"
    assert folders["2"].logical_id.path == ["LOOKER", "1", "2"]
    assert folders["3"].hierarchy_info.name == "folder3"
    assert folders["3"].logical_id.path == ["LOOKER", "1", "2", "3"]
