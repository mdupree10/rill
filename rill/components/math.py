from rill import *
from rill.fn import synced


@component
@inport('entry1')
@inport('entry2')
@outport('out')
def Add(entry1, entry2, out):
    for x, y in synced(entry1, entry2).iter_contents():
        out.send(x + y)
