---
subtitle: redgateCompare.sqlserver.options.ignores.ignoreDynamicDataMasking
---

## Description

Ignores `MASKED` clauses on table columns. Whilst masking-only differences will not be deployed, if your target column was masked and has any change deployed to it, for memory-optimized tables and scripts folders this will cause the field to lose its masking function regardless of whether it was also masked in the source database.

## Type

Boolean

## Default

`false`

## Usage

This setting can't be configured other than in a TOML configuration file.

### Flyway Desktop

This can be set from the comparison options settings in SQL Server projects.

### TOML Configuration File

```toml
[redgateCompare.sqlserver.options.ignores]
ignoreDynamicDataMasking = true
```
