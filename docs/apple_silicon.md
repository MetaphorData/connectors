# Building for Apple Silicon

For packages that use native C extensions, `poetry install` will attempt to download and install an OS-specific [wheel](https://pythonwheels.com/). However, not every package is published with a pre-built C extension for Apple Silicon (`arm64` architecture). When the wheel is missing, `pip` will attempt to compile the extension as part of the installation. This requires XCode Command Line Tools, which should be installed as part of [Homebrew](https://brew.sh/).

This page lists additional steps needed in order to build some of these packages.

## pymssql

1. Install required homebrew packages:
    ```
    brew install FreeTDS openssl@1.1
    ```

2. Specify `LDFLAGS` & `CFLAGS` environment variables when running `poetry install`:
    ```
    LDFLAGS="-L$(brew --prefix)/opt/freetds/lib -L$(brew --prefix)/opt/openssl@1.1/lib" \
    CFLAGS="-I$(brew --prefix)/opt/freetds/include -I$(brew --prefix)/opt/openssl@1.1/include" \
    poetry install -E all
    ```
