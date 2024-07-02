from metaphor.looker.folder import FolderMetadata, build_directories


def test_build_directories(test_root_dir) -> None:

    folder_map = {
        "1": FolderMetadata(id="1", name="folder1", parent_id=None),
        "2": FolderMetadata(id="2", name="folder2", parent_id="1"),
        "3": FolderMetadata(id="3", name="folder3", parent_id="2"),
    }

    assert build_directories("1", folder_map) == ["1"]
    assert build_directories("2", folder_map) == ["1", "2"]
    assert build_directories("3", folder_map) == ["1", "2", "3"]
