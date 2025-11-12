# Cost of Travel Index

A county-level economic analysis tool for tracking travel costs across the United States.

## Overview

The Cost of Travel Index measures micro and macro economic changes in the travel industry by calculating the cost of a hypothetical three-day weekend in each U.S. county. Similar to cost-of-living indices, this project tracks a standardized "basket of goods" representing typical travel expenses over time.

## Purpose

- Help destinations understand their relative positioning against competitive sets
- Track travel cost trends at county, state, regional, and national levels
- Support destination marketing organizations in positioning and strategy
- Provide data-driven insights into traveler decision-making factors

## Methodology

The index calculates monthly travel costs based on:

- **Lodging**: 2 nights at Friday/Saturday average rates
- **Dining**: 6 meals (4 at 35th percentile, 2 at 65th percentile)
- **Attractions**: 2 days of average spending
- **Retail**: 1 day of average spending

All calculations are performed at the county level using robust statistical methods (P5/P98 outlier removal) to reflect local market conditions.

## Technical Stack

- **Platform**: Jupyter/Colab Notebook
- **Languages**: Python, SQL
- **Data Source**: Zartico's BigQuery datasets
- **Time Period**: Monthly data from 2022-01-01 forward

## Project Status

**Current Version**: 0.1.0 (Planning Phase)

This project is in active development. See [Project_Start/project_overview.md](Project_Start/project_overview.md) for detailed methodology and technical specifications.

## Repository Structure

```
Cost_of_Travel_Index/
├── Project_Start/
│   └── project_overview.md    # Detailed project documentation
└── README.md                   # This file
```

## Out of Scope (v0.1.0)

- Visualization dashboards
- Automated pipeline scheduling
- Cohort identification tools
- Indexed baseline comparisons

## License

Private - Zartico Internal Project
