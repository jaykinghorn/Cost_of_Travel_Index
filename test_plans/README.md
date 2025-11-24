# Test Plans Directory

This directory contains test plans for methodology experiments and variations for the Cost of Travel Index.

## Active Tests

### Retail Daily Cap Test
**Branch**: `feature/retail-daily-cap-test`
**File**: [retail_daily_cap_test.md](retail_daily_cap_test.md)
**Status**: In Progress

Testing a modified approach to retail spending calculation that caps the number of retail transactions per cardholder per day before calculating statistics. This aims to prevent inflated median retail spending caused by high-frequency shoppers.

**Key Changes**:
- Cap at 6 transactions per cardholder per day
- Keep highest-value transactions when capping
- Apply outlier removal after capping

## Test Workflow

### 1. Create Test Branch
```bash
git checkout main
git pull
git checkout -b feature/test-name
```

### 2. Document Test Plan
Create a test plan document in this directory with:
- Problem statement
- Hypothesis
- Proposed methodology changes
- Implementation details
- Success criteria
- Comparison approach

### 3. Implement Changes
Update relevant files:
- `development_plan.md` - Document methodology changes
- Notebook code - Implement test logic
- Configuration - Add test parameters

### 4. Run Test
- Execute on test dataset (typically single month)
- Document results
- Compare with baseline methodology

### 5. Validate & Decide
- Review test results against success criteria
- Multi-month validation if initial test successful
- Merge to main or iterate based on findings

### 6. Merge or Archive
- **If successful**: Merge to main, update docs, close branch
- **If unsuccessful**: Document learnings, archive branch

## Test Documentation Template

Each test plan should include:

```markdown
# Test Name

**Branch**: feature/branch-name
**Version**: X.X.X
**Date**: YYYY-MM-DD
**Status**: Planning | Testing | Validating | Completed | Archived

## Problem Statement
[What issue are we trying to solve?]

## Hypothesis
[What do we think will fix it?]

## Proposed Modification
[Current approach vs. test approach]

## Implementation Details
[SQL/Python code changes]

## Testing Approach
[How will we validate?]

## Success Criteria
[What defines success?]

## Results
[To be filled after testing]

## Decision
[Merge, iterate, or archive?]
```

## Guidelines

### When to Create a Test Branch
- Testing significant methodology changes
- Exploring alternative statistical approaches
- Evaluating different data quality thresholds
- Comparing category definitions (MCC codes)

### When NOT to Use Test Branch
- Bug fixes (fix on main)
- Documentation updates (update on main)
- Configuration parameter tuning (can test on main)
- Minor code refactoring (update on main)

## Version Control

- **Main branch**: Production-ready methodology
- **Test branches**: Experimental variations
- **Merged tests**: Successful experiments integrated into main
- **Archived branches**: Documented but not merged

## Contact

Questions about test methodology? See project maintainer.
