---
title: "10. Deployment"
date: 2026-05-20
weight: 10
chapter: false
pre: <b>10. </b>
---

# 10. Deployment

> **Reference content — placeholder.** Prerequisites, stack order, parameters, and the verification recipe live here. For the hands-on path, see [Workshop chapter 3 — Deploy](../workshop/3-deploy/).

## How it deploys

Every push to `main` triggers `sdlf-ott-cicd` CodePipeline (4 stages: Source → Validate → Deploy → Notify). The Deploy stage runs `aws cloudformation deploy` for each of the 11 OTT stacks in dependency order.

**Stack order** (deployed by `sdlf-cicd/buildspec-deploy.yml`):

```
1.  pipeline-ott-glue-job        — Glue job + raw/curated catalog
2.  pipeline-ott-mainA           — Stage A state machine
3.  pipeline-ott-mainB           — Stage B state machine
4.  pipeline-ott-dataquality     — Curated DQ SM
5.  pipeline-ott-lutrefresh      — LUT-Refresh Lambda + DLQ
6.  pipeline-ott-contentgap      — Content Gap Lambda + 5 catalog tables + DLQ
7.  pipeline-ott-trending        — Trending Lambda + gold catalog + DLQ
8.  pipeline-ott-goldquality     — Gold DQ SM
9.  pipeline-ott-monitoring      — CloudWatch dashboard + 14 alarms
10. pipeline-ott-lakeformation   — Column-level RBAC on curated
11. pipeline-ott-api             — HTTP API
```

> **Internals deep-dive**: see [`sdlf-cicd/README.md`](https://github.com/thanhtrung102/ott-sdlf/blob/main/sdlf-cicd/README.md) for the buildspecs, the CodeBuild IAM role (including the `glue:UpdateDatabase` gotcha), and the failure-recovery playbook.
