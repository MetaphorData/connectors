# How to write a test for GX

A script that runs `Great Expectations` should be put into a script called `run.py` in a subdirectory here. The `run.py` file should have the following capabilities:
1. Has a function named `run` that removes the existing gx context files and runs the gx stuff:
```python
def run() -> None:
    current_path = Path(__file__).parent.resolve()

    if (current_path / "gx").exists():
        shutil.rmtree(current_path / "gx")
```
2. When instatiating gx context, `mode` should be `"file"` and `project_root_dir` should point to the directory `run.py` is in:
```python
    # Create Data Context.
    context = gx.get_context(mode="file", project_root_dir=current_path)
```
