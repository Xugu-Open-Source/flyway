---
subtitle: Model
---

## Description

The `model` command allows differences found by running the diff command to be applied to the schema model.
The schema model must be one of the comparison sources used in the diff artifact provided to `model`.

See [Schema Model](https://documentation.red-gate.com/display/FD/Schema+Model) for more information.

## Usage examples

### Saving changes from a database to the schema model

The `diff` command must first be run to discover the changes (see `diff.artifactFilename`) that can be saved to the schema model. The `diff` command can be combined with `model` into a single flyway call with verb chaining.

For example, the following command generates a diff between a database environment called `dev` and the schema model, saving the changes back into the schema model:

<pre class="console">&gt; flyway diff model -diff.source=dev -diff.target=schemaModel</pre>

## Parameters

### Optional

| Parameter                                                                                                                       | Namespace | Description                           |
|---------------------------------------------------------------------------------------------------------------------------------|-----------|---------------------------------------|
| [`artifactFilename`](<Configuration/Flyway Namespace/Flyway Model Namespace/Flyway Model Artifact Filename Setting>) | model     | The path to the diff artifact.        |
| [`changes`](<Configuration/Flyway Namespace/Flyway Model Namespace/Flyway Model Changes Setting>)                    | model     | A comma separated list of change ids. |

Universal commandline parameters are listed [here](<Command-line Parameters>).

## JSON output format

```json
{
  "messages" : [ ],
  "includedDependencies" : [ ],
  "filesChanged" : [ "C:\\Users\\Project\\schema-model\\MySchema\\Tables\\country.rgm" ]
}
```


## Error codes

This command can produce the following error codes:
- [Generic error codes](<Exit codes and error codes/General error codes>)
- [Comparison error codes](<Exit codes and error codes/Comparison error codes>)