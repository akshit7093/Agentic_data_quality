# TODO List

- [ ] Create a separate specialized agent for deep-dive individual column analysis (schema only, BI, full check autonomous). It should individually analyze each column, identify missing values (like the email nulls), and generate/check/validate every possible rule/combination for that column. \medium\ — 2026-03-01
- [ ] Agent missed nulls in email; needs deep dive column analysis. Create separate agent that looks at each column individually -> creates every possible rule -> generates code -> validates -> decides. Support schema only, BI, full check modes. high - 2026-03-01
