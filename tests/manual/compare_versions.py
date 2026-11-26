#!/usr/bin/env python3
"""Compare what two xmilib checkouts extract from a list of XMI/AWS files.

Manual pre-release check, not part of the unit tests. For every file it
records each dataset/member with its data length and the MD5 of its data
and converted text, in both versions, and prints the differences.

    git worktree add /tmp/xmi-old v1.1.1
    python3 tests/manual/compare_versions.py /tmp/xmi-old . files.txt

files.txt has one path per line. Each version runs in its own
subprocess, so the two copies of the ``xmi`` package never mix.
"""
import json
import subprocess
import sys

DUMP = r'''
import sys, hashlib, json
sys.path.insert(0, sys.argv[1])
import xmi
out = {}
for f in open(sys.argv[2]).read().splitlines():
    if not f:
        continue
    try:
        o = xmi.XMIT(filename=f, quiet=True)
        o.open()
        files = o.xmit['file'] if o.has_xmi() else o.tape['file']
        r = {}
        for ds in o.get_files():
            items = ([(ds + '(' + m + ')', files[ds]['members'][m]) for m in o.get_members(ds)]
                     if o.is_pds(ds) else [(ds, files[ds])])
            for name, d in items:
                data = d.get('data', b'')
                data = data if isinstance(data, (bytes, bytearray)) else b''
                r[name] = [len(data), hashlib.md5(data).hexdigest(),
                           hashlib.md5(d.get('text', '').encode()).hexdigest()]
        out[f] = r
    except Exception as e:
        out[f] = 'ERROR {}: {}'.format(type(e).__name__, e)
json.dump(out, sys.stdout)
'''


def dump(checkout, listfile):
    p = subprocess.run([sys.executable, '-c', DUMP, checkout, listfile],
                       capture_output=True, text=True, check=True)
    return json.loads(p.stdout)


def main():
    if len(sys.argv) != 4:
        sys.exit(__doc__)
    old_dir, new_dir, listfile = sys.argv[1:]
    old, new = dump(old_dir, listfile), dump(new_dir, listfile)
    errors = lambda d: sum(isinstance(v, str) for v in d.values())
    print('files: {}  errors old: {}  new: {}'.format(len(old), errors(old), errors(new)))
    bad = 0
    for f in old:
        a, b = old[f], new.get(f)
        if a == b:
            continue
        bad += 1
        if isinstance(a, str) or isinstance(b, str):
            print('{}\n  old: {}\n  new: {}'.format(f, a if isinstance(a, str) else 'ok',
                                                   b if isinstance(b, str) else 'ok'))
            continue
        print(f)
        for k in sorted(set(a) | set(b)):
            if a.get(k) != b.get(k):
                print('  {}: old {} new {}'.format(k, a.get(k), b.get(k)))
    print('differing files: {}'.format(bad))
    sys.exit(1 if bad else 0)


if __name__ == '__main__':
    main()
