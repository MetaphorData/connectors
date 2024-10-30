# GX test cases

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

When running `run.py`, remember to substitute the connection string (which has sensitive information inside!) with some bogus value in `great_expectations.yml`. The crawler can still parse it if the values are fake.
