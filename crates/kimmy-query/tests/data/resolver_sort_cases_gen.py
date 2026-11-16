"""Rebuild resolver_sort_cases.rs from the reference recordings.

    KIMMY_REFERENCE=<reference-6.0.5 directory> python3 resolver_sort_cases_gen.py out.rs
    rustfmt --edition 2024 out.rs && mv out.rs ../query_conformance/resolver_sort_cases.rs

Reads q7s3f, q7s3g, q7s3i and q7s3j (.jsonl).
"""
import json, sys, hashlib
from resolver_cases_gen import rs, docrs, outlines, R, unstable

# Cases whose ORDER the reference does not keep between two runs (reference-6.0.5/unstable.tsv),
# compared as a set: only a `rows all` case (every row of an aggregation) is built here.
UNSTABLE=unstable()
for _case,(_kind,_what) in UNSTABLE.items():
    if _case.startswith('q7s3') and (_kind,_what)!=('rows','all'):
        raise SystemExit('%s is unstable as %s %s: the generator takes only rows/all'%(_case,_kind,_what))
def rec_list(sel):
    docsets={}; recs=[]
    for s in sorted({x[0] for x in sel}):
        rows=[json.loads(l) for l in open(R+f'q7s3{s}.jsonl')]
        ol=outlines(s)
        for (ss,i) in sel:
            if ss!=s: continue
            r=rows[i-1]
            lab=r['label'].split()[0]
            c=r['command']; kind=r['kind']; rep=r['reply']
            before=r['before']
            h=hashlib.md5(json.dumps(before).encode()).hexdigest()[:8]
            docsets.setdefault(h,before)
            if 'error' in rep:
                out='Outcome::Refused(%s)'%rep['error']['code']
            elif kind=='find':
                out='Outcome::Docs(vec![%s])'%', '.join(str(int(x['$numberInt'])) for x in rep['ids']) if False else 'Outcome::Ids(vec![%s])'%', '.join(str(int(x['$numberInt'])) for x in rep['ids'])
            elif kind=='aggregate':
                out='Outcome::%s(vec![%s])'%('DocsAsSet' if lab in UNSTABLE else 'Docs', ', '.join(docrs(d) for d in rep['docs']))
            elif kind=='update':
                out='Outcome::Docs(vec![%s])'%', '.join(docrs(d) for d in r['after'])
            else: raise Exception(kind)
            if kind=='find':
                run='Run::Find { filter: %s, sort: %s }'%(docrs(c.get('filter',{})),docrs(c['sort']))
            elif kind=='aggregate':
                run='Run::Pipeline(vec![%s])'%', '.join(docrs(st) for st in c['pipeline'])
            else:
                run='Run::Update { query: %s, update: %s }'%(docrs(c['q']),docrs(c['u']))
            recs.append((lab,ol[lab],h,run,out))
    return docsets,recs
def emit(docsets,recs,name):
    out=[]
    for h,before in docsets.items():
        out.append('fn docs_%s() -> Vec<Document> {\n    vec![\n%s\n    ]\n}\n'%(h,'\n'.join('        %s,'%docrs(d) for d in before)))
    out.append('/// The number of cases `%s` holds, written by the generator.\npub const %s_COUNT: usize = %d;\n\n'%(name,name.upper(),len(recs)))
    out.append('pub fn %s() -> Vec<Rec> {\n    vec!['%name)
    for lab,line,h,run,o in recs:
        out.append('        Rec {\n            label: "%s",\n            line: %d,\n            docs: docs_%s,\n            run: %s,\n            outcome: %s,\n        },'%(lab,line,h,run,o))
    out.append('    ]\n}\n')
    return '\n'.join(out)
def ok(s,i):
    rows=[json.loads(l) for l in open(R+f'q7s3{s}.jsonl')]
    r=rows[i-1]
    try:
        [docrs(d) for d in r['before']]
        c=r['command']
        if r['kind']=='find': rs(c.get('filter',{})); rs(c['sort'])
        elif r['kind']=='aggregate': [docrs(x) for x in c['pipeline']]; [docrs(d) for d in r['reply'].get('docs',[])]
        else: docrs(c['q']); docrs(c['u']); [docrs(d) for d in r['after']]
        return True
    except Exception as e:
        print('skip',s,i,e,file=sys.stderr); return False
def sel():
    out=[]
    # sorts in find: f 27-40, 46-47, 53-54,60-61,67-68,74-75,81-82,88-89,95-96,102-103,109-110,116-117,123-124,130-131 ; g 93-100,105-127,131 ; j 151,152,155,156..: finds with a sort and filter/sort only
    for s in 'fgj':
        rows=[json.loads(l) for l in open(R+f'q7s3{s}.jsonl')]
        for i,r in enumerate(rows,1):
            if r['kind']!='find': continue
            c=r['command']; ks=set(c)
            if 'sort' in ks and ks<= {'filter','sort'}:
                if s=='f' and not (27<=i<=40 or 46<=i<=131): continue
                if s=='g' and (not (93<=i<=131) or i in (98,99)): continue   # g:98, 99 order documents: the document order commit
                if s=='j' and not (151<=i<=174): continue
                if ok(s,i): out.append((s,i))
    # i: aggregates 34-49, 68-75 ; updates 76-95
    for i in list(range(34,96)):
        if ok('i',i): out.append(('i',i))
    # j: 187-209 (sortArray, group top, updates)
    for i in range(187,210):
        if ok('j',i): out.append(('j',i))
    return out
if __name__=='__main__':
    ds,recs=rec_list(sel())
    hdr='''//! Rebuilt by `tests/data/resolver_sort_cases_gen.py`; its docstring has the command.
//! Generated from the recordings q7s3f, q7s3g, q7s3i and q7s3j (reference 6.0.5): the sorts
//! over arrays, 16746, `$sortArray` and `$push`'s `$sort`. The label is the case's, the line
//! its place in the set's `.out`.

use super::resolver_sorts::{Outcome, Rec, Run};
use bson::{Bson, Document, doc};

'''
    open(sys.argv[1],'w').write(hdr+emit(ds,recs,'cases_sorts'))
    print(len(recs))
