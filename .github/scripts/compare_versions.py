# This script checks if current version is larger than latest main version.

import sys

from packaging import version

main_tag = sys.argv[1]
current_version = sys.argv[2]

# We want this program to exit 1 when the check fails.
# Since `True` == `1`, we want this comparison to evaluate to `0` if we
# want the check to pass.
sys.exit(version.parse(main_tag) >= version.parse(current_version))
