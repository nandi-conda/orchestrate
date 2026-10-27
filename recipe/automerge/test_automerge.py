"""The automerge.core calls orchestrator/control/sync.py makes, against the
built package: a document written, saved, loaded and synced to another."""
from automerge.core import ROOT, Document, Message, ObjType, ScalarType, SyncState

a = Document()
with a.transaction() as tx:
    tx.put(ROOT, "k", ScalarType.Str, "v")
b = Document.load(a.save())
got = b.get(ROOT, "k")
assert got is not None and not isinstance(got[0], ObjType) and got[0][1] == "v", got
assert list(b.keys(ROOT)) == ["k"]

c = Document()
sa, sc = SyncState(), SyncState()
for _ in range(10):
    m = a.generate_sync_message(sa)
    if m is not None:
        c.receive_sync_message(sc, Message.decode(m.encode()))
    n = c.generate_sync_message(sc)
    if n is not None:
        a.receive_sync_message(sa, Message.decode(n.encode()))
    if m is None and n is None:
        break
assert c.get(ROOT, "k")[0][1] == "v"
print("automerge ok")
