---
subtitle: redgateCompare.sqlserver.data.options.comparison.treatEmptyStringAsNull
---

## Description

When this option is selected, empty strings (no characters) will be treated as `NULL`.

If you want to treat fixed-length fields containing only whitespace as `NULL`, you must also enable the 'Trim trailing spaces' option.

## Type

Boolean

## Default

`false`

## Usage

This setting can't be configured other than in a TOML configuration file.

### Flyway Desktop

This can be set from the data comparison options settings in SQL Server projects.

### TOML Configuration File

```toml
[redgateCompare.sqlserver.data.options.comparison]
treatEmptyStringAsNull = true
```
