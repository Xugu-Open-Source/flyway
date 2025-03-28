---
subtitle: flyway.deploy.scriptFilename
---

## Description

The path to the script that will be deployed.
This will be resolved relative to the [working directory](<Command-line Parameters/Working Directory Parameter>).

## Type

String

## Default

`deployments/D__deployment.sql`

## Usage

### Flyway Desktop

This can't be added to a configuration file via Flyway Desktop.

### Command-line

```bash
./flyway deploy -scriptFilename="output.sql"
```

### TOML Configuration File

```toml
[flyway.deploy]
scriptFilename = "output.sql"
```
