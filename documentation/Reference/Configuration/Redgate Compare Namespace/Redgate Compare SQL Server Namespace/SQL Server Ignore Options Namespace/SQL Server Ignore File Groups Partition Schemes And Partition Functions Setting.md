---
subtitle: redgateCompare.sqlserver.options.ignores.ignoreFileGroupsPartitionSchemesAndPartitionFunctions
---

## Description

Ignores filegroup clauses, partition schemes and partition functions on tables and keys when comparing and deploying databases. Partition schemes and partition functions are not displayed in the comparison results.

## Type

Boolean

## Default

`true`

## Usage

This setting can't be configured other than in a TOML configuration file.

### Flyway Desktop

This can be set from the comparison options settings in SQL Server projects.

### TOML Configuration File

```toml
[redgateCompare.sqlserver.options.ignores]
ignoreFileGroupsPartitionSchemesAndPartitionFunctions = true
```
