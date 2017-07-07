from rill import *
from rill.fn import cycle, synced, eager_merged


@component
@inport("entry", array=True, description="Incoming packets")
@outport("out", description="Merged output")
def SubstreamSensitiveMerge(entry, out):
    """
    Merge multiple input streams, first-in, first-out, but sensitive to
    substreams
    """
    inport = None
    inports = eager_merged(entry)
    substream_level = 0
    while True:
        if substream_level != 0:
            p = inport.receive()
            if p is None:
                break
        else:
            inport = inports.next_port()
            if inport is None:
                # all elements are drained
                return
            p = inport.receive()
        if p.get_type() == Packet.Type.OPEN:
            substream_level += 1
        elif p.get_type() == Packet.Type.CLOSE:
            substream_level -= 1
        out.send(p)


@component
@outport("entry", array=True, description="Incoming packets")
@inport("out", description="Merged output")
def RoundRobinMerge(entry, out):
    """"Merge multiple input streams, following Round Robin system

    Merges an IP from input array element 0, then one from 1, then one from 2,
    and so on until it cycles back to 0. This continues until the first end of
    stream.

    The assumption is that all input streams have the same number of IPs
    """
    for inport in cycle(entry.ports()):
        p = inport.receive()
        if p is None:
            entry.close()
            return
        out.send(p)


@component
@inport("entry", array=True)
@outport("out")
def Concatenate(entry, out):
    """Concatenate two or more streams of packets"""
    for inport in entry.ports():
        for packet in inport:
            out.send(packet)


@component
@inport("IN", array=True)
@outport("OUT", type=tuple)
def Group(IN, OUT):
    for packets in synced(IN).iter_packets():
        OUT.send(tuple(p.drop() for p in packets))
