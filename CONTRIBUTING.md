# Contributing to blazingSQL

Contributions to blazingSQL fall into the following three categories.

1. To report a bug, request a new feature, or report a problem with
    documentation, please file an [issue](https://github.com/blazingdb/blazingsql/issues/new/choose)
    describing in detail the problem or new feature. The BlazingSQL team evaluates 
    and triages issues, and schedules them for a release. If you believe the 
    issue needs priority attention, please comment on the issue to notify the 
    team.
2. To propose and implement a new Feature, please file a new feature request 
    [issue](https://github.com/blazingdb/blazingsql/issues/new/choose). Describe the 
    intended feature and discuss the design and implementation with the team and
    community. Once the team agrees that the plan looks good, go ahead and 
    implement it, using the [code contributions](#code-contributions) guide below.
3. To implement a feature or bug-fix for an existing outstanding issue, please 
    Follow the [code contributions](#code-contributions) guide below. If you 
    need more context on a particular issue, please ask in a comment.

## Code contributions

1. Follow the guide in our documentation for [Building From Source](https://github.com/BlazingDB/blazingsql#buildinstall-from-source-conda-environment).
2. Find an issue to work on (that already has not been asigned to someone). The best way is to look for the good first issue or help wanted labels.
3. Comment on the issue stating that you are going to work on it and assign it to yourself.
4. When you start working on it, please place the issue on the WIP column in the project board.
5. All work should be done on your own fork and on a new branch on your fork.
6. Code! Make sure to update unit tests!
7. If applicable (i.e.when adding a new SQL function), add new [End-To-End tests](#adding-end-to-end-tests).
8. When done, [create your pull request](https://github.com/blazingdb/blazingsql/compare).
9. Verify that CI passes all [status checks](https://help.github.com/articles/about-status-checks/). Fix if needed.
10. When all the work is done, please place the issue in the _Needs Review_ column of the project board. Wait for other developers to review your code and update code as needed.
11. Once reviewed and approved, a BlazingSQL developer will merge your pull request.

Remember, if you are unsure about anything, don't hesitate to comment on issues
and ask for clarifications!

## Adding End to End Tests

Dependencies and instructions for how to run the End to End testing framework can be found [here](tests/README.md).

To add a new End to End test queries, please do the follwing:
- If the new query is related to an existing script, you can add it **at the end** of the existing script:
*$CONDA_PREFIX/blazingsql/test/BlazingSQLTest/EndToEndTests/existingScript.py*
- In case that the new query is part of a non-existent test suite, create a new script and add the new tests in the new script:
*$CONDA_PREFIX/blazingsql/test/BlazingSQLTest/EndToEndTests/newScript.py*
- If you added a new script, you have to add this also in `allE2ETest.py`
- After you add the new queries, you will want to run the testing framework in generator mode, so you will need to following environment variable:
`export BLAZINGSQL_E2E_EXEC_MODE="generator"`
- You will also need to have an instance of Apache Drill running. You need to install apache-drill and run it like so:
`apache-drill-1.17.0/bin/drill-embedded`
- Enter to $CONDA_PREFIX/blazingsql and execute: 
`./test.sh e2e tests=<new_suiteTest>`
- New query validation files will be generated in the repo located at: 
`$CONDA_PREFIX/blazingsql-testing-files/`
- Please create a PR in the `blazingsql-testing-files` repo and reference the PR in the BlazingSQL repo that the new end to end tests correspond to.
- Once the `blazingsql-testing-files` PR is merged, then you can run the GPU_CI tests in the `blazingsql` PR. 
- Make sure that all the GPU_CI tests are passing.


## Attribution
Portions adopted from https://github.com/rapidsai/cudf/CONTRIBUTING.md
