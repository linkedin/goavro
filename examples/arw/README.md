# arw

Avro ReWrite

Provide command line utility to rewrite an Avro Object Container File
(OCF), while changing the block count, the compression algorithm, or
upgrading the schema. Note that when upgrading the schema, the new
schema must be able to properly encode the data read using the old
schema.

Why would a person want to upgrade the schema for an existing OCF?
Perhaps if one wants to append data to it using the new schema.

Example use:

```
arw -summary -bc 100 -compression deflate -schema new-schema.avsc source.avro destination.avro
```

If summary option, `-summary`, is provided, `arw` will provide summary
information while rewriting the OCF.

If verbose option, `-v`, is provided, `arw` will provide verbose
information while rewriting the OCF. Specifying verbose implies the
summary option.

If block count option, `-bc`, is provided, then each block will have
no more items than specified. If omitted, then `arw` will re-encode
blocks of the same length as found in `source.avro`. For instance, if
the first block had 10 items, and the second has 15, then the
`destination.avro` file will also have 10 items in the first block and
15 items in the second block.

If compression option is omitted, then `arw` will use the same
compression algorithm as found in `source.avro`.

If schema option is omitted, then `arw` will write the new Avro file
using the same schema as found in `source.avro`. If provided, `arw`
will read the source Avro file using its provided schema, but attempt
to encode and write the destination Avro file using the newly provided
schema. If an item fails to encode using the new schema, the process
will be aborted and an error message will be provided.

If `source.avro` is a hyphen character, `-`, then `arw` will read from
standard input.  If `destination.avro` is a hyphen character, then
`arw` will write to standard output.

Invoking `arw` without any of the options simply copies the OCF file,
verifying the contents of the data along the way.
