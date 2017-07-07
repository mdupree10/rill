from rill import *


@component
@inport('entry')
@outport('out')
def First(entry, out):
    """
    Pass along only the first packet in the stream.
    """
    value = entry.receive_once()
    out.send(value)


@component
@outport("acc")
@outport("rej", required=False)
@inport("entry")
@inport("number", required=True)
def SelNthItem(entry, number, acc, rej):
    """Select from IN one packet by NUMBER (0 means first), sending via ACC,
    rejected packets via REJ"""
    selector = number.receive_once()

    for i, p in enumerate(entry):
        if i == selector:
            acc.send(p)
        else:
            rej.send(p)
