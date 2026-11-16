"""Rebuild resolver_order_cases.rs from the reference recordings.

    KIMMY_REFERENCE=<reference-6.0.5 directory> python3 resolver_order_cases_gen.py out.rs
    rustfmt --edition 2024 out.rs && mv out.rs ../query_conformance/resolver_order_cases.rs

Reads q7s3g, q7s3h and q7s3j (.jsonl).
"""
import json, sys
import resolver_sort_cases_gen as gen2
from resolver_sort_cases_gen import rec_list, emit, ok
from resolver_cases_gen import R
def sel():
    out=[]
    for i in range(1,113):
        if ok('h',i): out.append(('h',i))
    for i in range(210,370):
        if ok('j',i): out.append(('j',i))
    for i in (98,99):
        if ok('g',i): out.append(('g',i))
    return out
ds,recs=rec_list(sel())
hdr='''//! Rebuilt by `tests/data/resolver_order_cases_gen.py`; its docstring has the command.
//! Generated from the recordings q7s3g, q7s3h and q7s3j (reference 6.0.5): documents compared with
//! documents, by the sort, `$cmp`, `$max`, `$min`, `$group` and the update operators. The label is
//! the case's, the line its place in the set's `.out`.

use super::common::dec;
use super::resolver_sorts::{Outcome, Rec, Run};
use bson::{Bson, Document, doc};

'''
open(sys.argv[1],'w').write(hdr+emit(ds,recs,'cases_document_order'))
print(len(recs))
