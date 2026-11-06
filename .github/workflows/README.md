# Deployment-only automation

Verification runs on the designated verification host under the native resource guard before delivery.
There are no automatic CI tests in this repository. The removed ci.yml test
workflow is replaced by scripts/verify-locally.sh, which runs the executable
checks directly. A green fabricated/skipped CI job is not a replacement.

A separately requested deployment may add an explicit workflow_dispatch-only
deployment workflow. It must not run the test suite. Protected delivery uses
the actual guarded verification evidence and independent acceptance for the
exact head and merge candidate; preserve pull-request and merge-queue rules.
