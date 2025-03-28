---
pill: gcsmProject
subtitle: environments.*.resolvers.gcsm.project
---

{% include enterprise.html %}

## Description

The version of the secret engine.

## Type

String

## Default

<i>none</i>

## Usage

### Flyway Desktop

This can't be set in a config file via Flyway Desktop, although it will be honoured, and it can be configured as an advanced parameter in operations on the Migrations page.

### Command-line

```bash
./flyway info -environments.development.resolvers.vault.engineVersion="v2"
```

### TOML Configuration File

```toml
[environments.development.resolvers.vault]
engineVersion = "v2"
```
