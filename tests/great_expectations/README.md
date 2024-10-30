# GX test cases

A test case in the unit test is a directory that has a file named `run.py`. In the file, there should be a `run` method that cleans up the `gx` directory, creates the context in the directory and use `file` as the mode. In other words, it should look like this:

```python
import great_expectations as gx
import shutil
from pathlib import Path

def run() -> None:

    # Clear gx context.
    if (current_path / "gx").exists():
        shutil.rmtree(current_path / "gx")

    # Create Data Context.
    context = gx.get_context(mode="file", project_root_dir=current_path)

    # ... snipped ...

if __name__ == "__main__":
    run()
```

Running `run.py` in each directory will regenerate the `gx` directory, which is where GX artifacts are stored.

## Snowflake

For Snowflake, make sure you have a `config.yml` file that has the following fields:

```yaml
user: user
password: pwd
account: acc
role: role
warehouse: wh
```

### CI

Pass environment variables so that the test can run.
