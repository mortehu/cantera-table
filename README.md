Cantera Table
=============

Cantera Table a forward and inverted document index with compact storage, with
a rich query language, and the ability to sort search results by arbitrary
metrics.

# Introduction

To use this software, you need a schema file.  This is a simple ASCII file,
designed to be editable with an ordinary text editor.  Here's an example:

    summary          /var/search/summary:html  0
    summary          /var/search/summary:pdf   1000000000
    summary-override /var/search/summary.override
    index            /var/search/index:html
    index            /var/search/index:pdf

The supported table types are `summary`, `summary-override`, and `index`.  What
follows is a brief description of each type.

# Summary tables

A summary table consists of pairs of strings.  The first string is the name of
a document, and the second string is a JSON object describing the document.  If
summaries for the same document exist in multiple summary tables, the JSON
objects are merged.

The data in all summary tables are mapped to a single 64 bit address space.
The integer value in the third column of the schema file indicates the address
offset for the summary table -- all addresses within the given table are
shifted by the specified amount.

During search queries, summary tables are accessed by seeking directly to the
correct file offset.  During index construction, offsets are found using the
hash map stored at the end of each file.

# Summary-override tables

Sometimes a document changes, and you want the changes to be visible without
rebuilding all index tables.  This is when you use a summary-override table.
They are stored in the exact same format as summary tables.  When a search
query has found its result set, the summary-override tables are queried for
entries with matching names.  If an entry is found, it is used instead of the
entry from the summary table.

When a summary-override table is used, it causes a hash map lookup for every
query result entry.  It's therefore advisable that the summary-override tables
are kept in RAM.

# Index tables

An index table consists of strings coupled with an array of integer/floating
point value pairs.  The integers represent offsets into the summary table
address space.  The floating point values represent relevance, dates or any
other data you might want to rank or filter your search queries by.

During search queries, index tables are accessed by resolving keywords into
file offsets using the hash map stored at the end of each file.

# How to build and use an inverted index

  1. Create a file named /var/search/schema with the following contents:

         summary /var/search/summary
         index /var/search/index

  2. Create the summary table input data by building one or more text files
     with each line in the following format:

         <DOCUMENT-NAME> TAB <DESCRIPTION>

  3. Assemble the summaries into a summary table:

         $ LC_COLLATE=C sort /tmp/index-input/summary.* |
         > ca-load --output-type=summary /var/search/summary

  4. Create a keyword table by building one or more text files with each line
     in the following format:

         <KEYWORD> TAB <DOCUMENT-NAME> TAB <SCORE>

  5. Assemble the keywords into an index table:

         $ sort /tmp/index-input/keywords.*    |
         > ca-load --output-type=index         \
                   --schema=/var/search/schema \
                   /var/search/index

     Each document name will be converted to its offset according the the schema
     and the summary table(s).

  6. Issue the following command:

         $ ca-shell /var/search/schema

     You are now in a position to issue search queries.

# Query language

TODO(mortehu): Write this section.
