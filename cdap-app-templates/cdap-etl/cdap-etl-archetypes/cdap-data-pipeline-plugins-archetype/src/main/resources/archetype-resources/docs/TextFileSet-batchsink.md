# Text FileSet Batch Sink

Description
-----------

Writes to a CDAP FileSet in text format. One line is written for each record
sent to the sink. All record fields are joined using a configurable separator.


Use Case
--------

This source is used whenever you need to write to a FileSet in text format.

Properties
----------

**fileSetName:** The name of the FileSet to write to.

**fieldSeparator:** The separator to join input record fields on. Defaults to ','.

Example
-------

This example writes to a FileSet named 'users', using the '|' character to separate record fields:

    {
        "name": "TextFileSet",
        "type": "batchsink",
        "properties": {
            "fileSetName": "users",
            "fieldSeparator": "|"
        }
    }

In order to properly write to the FileSet, the runtime argument 'dataset.<name>.output.path' should be set.
In the example above, setting 'dataset.users.output.path' to 'run10' will configure the pipeline run to write
to the 'run10' directory in the FileSet.
