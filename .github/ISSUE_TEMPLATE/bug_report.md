---
name: Bug report
about: Create a report to help improve blazingSQL
title: "[BUG]"
labels: "bug,? - Needs Triage"
assignees: ''

---

**Describe the bug**
A clear and concise description of what the bug is.

**Steps/Code to reproduce bug**
Please provide the simplest and most complete steps or code that will allow us to reproduce the bug. These should be:
Minimal – Use as little code as possible that still produces the same problem
Complete – Provide all parts needed to reproduce the problem
Verifiable – Test the code you’re about to provide to make sure it reproduces the problem

**Expected behavior**
A clear and concise description of what you expected to happen.

**Environment overview (please complete the following information)**
 - Environment location: [Bare-metal, Docker, Cloud(specify cloud provider)]
 - Method of BlazingSQL install: [conda, Docker, or from source]
   - If method of install is [Docker], provide `docker pull` & `docker run` commands used
 - **BlazingSQL Version** which can be obtained by doing as follows:
   ```
   import blazingsql
   print(blazingsql.__info__())
   ```

**Environment details**
Please run and paste the output of the `print_env.sh` script here, to gather any other relevant environment details

**Additional context**
Add any other context about the problem here.

**----For BlazingSQL Developers----**
**Suspected source of the issue**
Where and what are potential sources of the issue

**Other design considerations**
What components of the engine could be affected by this?
