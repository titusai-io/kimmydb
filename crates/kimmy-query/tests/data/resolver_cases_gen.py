"""Rebuild resolver_cases.rs from the reference recordings.

    KIMMY_REFERENCE=<reference-6.0.5 directory> python3 resolver_cases_gen.py out.rs
    rustfmt --edition 2024 out.rs && mv out.rs ../query_conformance/resolver_cases.rs

Reads q7s3f, q7s3g and q7s3j (.jsonl and .out). Also the module the three other resolver generators import.
"""
import json, sys, hashlib, re
import os
R=os.environ['KIMMY_REFERENCE'].rstrip('/')+'/'

# The docs functions already written in this run: `emit` is called once for each list of cases, and
# a fixture that both lists use is written once (a second `fn docs_<hash>` is E0428).
_DOCS_WRITTEN = set()

# The cases whose reference output differs between two identical runs (the tester's unstable.tsv:
# case, set, kind, what, two proofs). A table must not assert the order of the UNSTABLE part.
# Falls back to the one case this repository knew before the file existed, and says so.
def unstable():
    try:
        rows=[l.rstrip('\n').split('\t') for l in open(R+'unstable.tsv') if l.strip() and not l.startswith('#')]
        return {r[0]:(r[2],r[3]) for r in rows}
    except FileNotFoundError:
        print('unstable.tsv is missing: using the built-in list (q7s3i0069 only)',file=sys.stderr)
        return {'q7s3i0069':('rows','all')}

def num(s):
    return s
def rs(v):
    if v is None: return 'Bson::Null'
    if v is True: return 'Bson::Boolean(true)'
    if v is False: return 'Bson::Boolean(false)'
    if isinstance(v,str): return 'Bson::String(%s.to_string())'%json.dumps(v)
    if isinstance(v,list): return 'Bson::Array(vec![%s])'%', '.join(rs(x) for x in v)
    if isinstance(v,dict):
        if '$numberInt' in v: return 'Bson::Int32(%s)'%v['$numberInt']
        if '$numberLong' in v: return 'Bson::Int64(%s)'%v['$numberLong']
        if '$numberDouble' in v:
            d=v['$numberDouble']
            m={'NaN':'f64::NAN','Infinity':'f64::INFINITY','-Infinity':'f64::NEG_INFINITY'}
            return 'Bson::Double(%s)'%m.get(d, d if ('.' in d or 'e' in d.lower()) else d+'.0')+''
        if '$numberDecimal' in v: return 'dec(%s)'%json.dumps(v['$numberDecimal'])
        if '$date' in v:
            d=v['$date']
            assert isinstance(d,dict) and '$numberLong' in d, d
            return 'Bson::DateTime(bson::DateTime::from_millis(%s))'%d['$numberLong']
        if '$minKey' in v: return 'Bson::MinKey'
        if '$maxKey' in v: return 'Bson::MaxKey'
        if any(k.startswith('$') for k in v) and len(v)<=2 and list(v)[0] in ('$oid','$regularExpression','$timestamp','$binary','$symbol','$undefined','$code'):
            raise Exception('unsupported '+str(v))
        return 'Bson::Document(%s)'%docrs(v)
    if isinstance(v,(int,float)): raise Exception('plain number %r'%v)
    raise Exception(repr(v))
def docrs(d):
    if not d: return 'Document::new()'
    return 'doc! { %s }'%', '.join('%s: %s'%(json.dumps(k),rs(x)) for k,x in d.items())
def outlines(s):
    res={}
    for i,l in enumerate(open(R+f'q7s3{s}.out')):
        m=re.match(r'--- (q7s3%s\d+) '%s,l)
        if m: res[m.group(1)]=i+1
    return res
def ids_of(r):
    rep=r['reply']
    if 'ids' in rep: return [int(x['$numberInt']) for x in rep['ids']]
    return None
def emit(sel, name):
    docsets={}
    cases=[]
    for s in sorted({x[0] for x in sel}):
        rows=[json.loads(l) for l in open(R+f'q7s3{s}.jsonl')]
        ol=outlines(s)
        for (ss,i) in sel:
            if ss!=s: continue
            r=rows[i-1]
            lab=r['label'].split()[0]
            c=r['command']
            before=r['before']
            key=json.dumps(before)
            h=hashlib.md5(key.encode()).hexdigest()[:8]
            docsets.setdefault(h,(lab,before))
            ids=ids_of(r)
            cases.append((lab,ol[lab],h,c['filter'],ids,c.get('sort')))
    out=[]
    for h,(lab,before) in docsets.items():
        if h in _DOCS_WRITTEN: continue
        _DOCS_WRITTEN.add(h)
        out.append('fn docs_%s() -> Vec<Document> {\n    vec![\n%s\n    ]\n}\n'%(h,'\n'.join('        %s,'%docrs(d) for d in before)))
    attr='#[allow(dead_code)] // read by the element tests of a later commit of the series\n' if name=='cases_elem_match' else ''
    out.append('/// The number of cases `%s` holds, written by the generator.\npub const %s_COUNT: usize = %d;\n\n'%(name,name.upper(),len(cases)))
    out.append(attr+'pub fn %s() -> Vec<Case> {\n    vec!['%name)
    for lab,line,h,flt,ids,sort in cases:
        idr='Some(vec![%s])'%', '.join(map(str,ids)) if ids is not None else 'None'
        out.append('        Case {\n            label: "%s",\n            line: %d,\n            docs: docs_%s,\n            filter: %s,\n            ids: %s,\n        },'%(lab,line,h,docrs(flt),idr))
    out.append('    ]\n}\n')
    return '\n'.join(out)
def has_op(x,op):
    if isinstance(x,dict): return op in x or any(has_op(v,op) for v in x.values())
    if isinstance(x,list): return any(has_op(v,op) for v in x)
    return False
def select():
    plain=[];em=[]
    for s in 'fgj':
        rows=[json.loads(l) for l in open(R+f'q7s3{s}.jsonl')]
        for i,r in enumerate(rows,1):
            if r['kind']!='find': continue
            c=r['command']; ks=set(c.keys())
            ok = ks=={'filter'} or (ks=={'filter','sort'} and c['sort']=={'_id':{'$numberInt':'1'}})
            if not ok: continue
            if 'reply' not in r: continue
            try:
                docrs_test=[docrs(d) for d in r['before']]; rs(c['filter'])
            except Exception as e:
                continue
            if s=='f' and not (i<=26 or 41<=i<=131 or 301<=i<=341): continue
            (em if has_op(c['filter'],'$elemMatch') else plain).append((s,i))
    return plain,em
if __name__=='__main__':
    plain,em=select()
    hdr='''//! Rebuilt by `tests/data/resolver_cases_gen.py`; its docstring has the command.
//! Generated from the recordings q7s3f, q7s3g and q7s3j (reference 6.0.5): every `find` that
//! takes a filter and no projection, limit or sort other than `{_id: 1}`. The label is the
//! case's, the line its place in the set's `.out`, and `ids` the ids the reference answered.

use super::common::{Case, dec};
use bson::{Bson, Document, doc};

'''
    body=emit(plain,'cases_filters')+'\n'+emit(em,'cases_elem_match')
    if 'dec(' not in body: hdr=hdr.replace('{Case, dec}','Case')
    open(sys.argv[1],'w').write(hdr+body)
    print(len(plain),len(em))
