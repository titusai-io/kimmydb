"""Rebuild resolver_element_cases.rs from the reference recordings.

    KIMMY_REFERENCE=<reference-6.0.5 directory> python3 resolver_element_cases_gen.py out.rs
    rustfmt --edition 2024 out.rs && mv out.rs ../query_conformance/resolver_element_cases.rs

Reads q7s3b, q7s3g and q7s3i (.jsonl).
"""
import json, sys, hashlib
from resolver_cases_gen import rs, docrs, outlines, R
def load(s): return [json.loads(l) for l in open(R+f'q7s3{s}.jsonl')]
def sel():
    out=[]
    ranges={'g':list(range(139,156))+list(range(174,183))+list(range(201,210))+list(range(228,237))+list(range(255,264))+list(range(282,331)),
            'i':list(range(161,187)),'b':list(range(155,181))}
    for s,rg in ranges.items():
        rows=load(s)
        for i in rg:
            if i>len(rows): continue
            r=rows[i-1]; c=r.get('command') or {}
            rep=r.get('reply',{})
            try:
                [docrs(d) for d in r['before']]
                if r['kind']=='find':
                    ks=set(c)
                    if not (ks<= {'filter','projection'}): continue
                    rs(c.get('filter',{})); 
                    if 'projection' in c: rs(c['projection'])
                    if 'projection' not in c and s!='b': continue   # plain filters are in cases_filters already
                    if s=='b' and 'projection' not in c:
                        pass
                elif r['kind']=='update':
                    if 'writeErrors' in rep: continue
                    docrs(c['q']); docrs(c['u']); [docrs(d) for d in c.get('arrayFilters',[])]; [docrs(d) for d in r['after']]
                else: continue
                out.append((s,i))
            except Exception as e:
                print('skip',s,i,e,file=sys.stderr)
    return out
def main(path):
    docsets={}; recs=[]
    byset={}
    for s,i in sel(): byset.setdefault(s,[]).append(i)
    for s,idx in byset.items():
        rows=load(s); ol=outlines(s)
        for i in idx:
            r=rows[i-1]; lab=r['label'].split()[0]; c=r['command']; rep=r['reply']
            h=hashlib.md5(json.dumps(r['before']).encode()).hexdigest()[:8]
            docsets.setdefault(h,r['before'])
            if r['kind']=='find':
                proj = docrs(c['projection']) if 'projection' in c else 'Document::new()'
                if 'error' in rep: out='Outcome::Refused(%s)'%rep['error']['code']
                elif 'projection' in c:
                    out='Outcome::Docs(vec![%s])'%', '.join(docrs(d) for d in rep['docs'])
                else:
                    out='Outcome::Ids(vec![%s])'%', '.join(str(int(x['$numberInt'])) for x in rep['ids'])
                run='Run::Find { filter: %s, projection: %s }'%(docrs(c.get('filter',{})),proj)
            else:
                af=', '.join(docrs(d) for d in c.get('arrayFilters',[]))
                run='Run::Update { query: %s, update: %s, array_filters: vec![%s], multi: %s }'%(docrs(c['q']),docrs(c['u']),af,'true' if c.get('multi') else 'false')
                out='Outcome::Docs(vec![%s])'%', '.join(docrs(d) for d in r['after'])
            recs.append((lab,ol[lab],h,run,out))
    o=[]
    for h,before in docsets.items():
        o.append('fn docs_%s() -> Vec<Document> {\n    vec![\n%s\n    ]\n}\n'%(h,'\n'.join('        %s,'%docrs(d) for d in before)))
    o.append('/// The number of cases `%s` holds, written by the generator.\npub const %s_COUNT: usize = %d;\n\n'%('cases_elements','CASES_ELEMENTS',len(recs)))
    o.append('pub fn cases_elements() -> Vec<Rec> {\n    vec![')
    for lab,line,h,run,out in recs:
        o.append('        Rec {\n            label: "%s",\n            line: %d,\n            docs: docs_%s,\n            run: %s,\n            outcome: %s,\n        },'%(lab,line,h,run,out))
    o.append('    ]\n}\n')
    hdr='''//! Rebuilt by `tests/data/resolver_element_cases_gen.py`; its docstring has the command.
//! Generated from the recordings q7s3b, q7s3g and q7s3i (reference 6.0.5): `$elemMatch`,
//! `arrayFilters` and the positional `$` of a projection and of an update. The label is the
//! case's, the line its place in the set's `.out`.

use super::resolver_elements::{Outcome, Rec, Run};
use bson::{Bson, Document, doc};

'''
    open(path,'w').write(hdr+'\n'.join(o))
    print(len(recs))
main(sys.argv[1])
