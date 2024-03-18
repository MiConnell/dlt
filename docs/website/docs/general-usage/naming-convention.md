---
title: Naming Convention
description: Control how dlt creates table, column and other identifiers
keywords: [identifiers, snake case, ]
---

# Naming Convention
`dlt` creates tables, child tables and column schemas from the data. The data being loaded,
typically JSON documents, contains identifiers (i.e. key names in a dictionary) with any Unicode
characters, any lengths and naming styles. On the other hand the destinations accept very strict
namespaces for their identifiers. Like Redshift that accepts case-insensitive alphanumeric
identifiers with maximum 127 characters.

Each schema contains `naming convention` that tells `dlt` how to translate identifiers to the
namespace that the destination understands.



## Default naming convention (snake_case)

1. Converts identifiers to **snake_case**, small caps. Removes all ascii characters except ascii
   alphanumerics and underscores.
1. Adds `_` if name starts with number.
1. Multiples of `_` are converted into single `_`.
1. The parent-child relation is expressed as double `_` in names.
1. It shorts the identifier if it exceed the length at the destination.

> 💡 Standard behavior of `dlt` is to **use the same naming convention for all destinations** so
> users see always the same tables and columns in their databases.

> 💡 If you provide any schema elements that contain identifiers via decorators or arguments (i.e.
> `table_name` or `columns`) all the names used will be converted via the naming convention when
> adding to the schema. For example if you execute `dlt.run(... table_name="CamelCase")` the data
> will be loaded into `camel_case`.

> 💡 Use simple, short small caps identifiers for everything!


## Change naming convention

The naming convention is configurable and users can easily create their own
conventions that i.e. pass all the identifiers unchanged if the destination accepts that (i.e.
DuckDB).

