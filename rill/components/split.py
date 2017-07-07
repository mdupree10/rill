from rill import *
from rill.fn import zip, cycle, load_balanced, forked


# TODO: add ability to control number of packets sent to each
@component
@inport("entry", description="Incoming packets")
@outport("out", array=True, description="Split output")
def RoundRobinSplit(entry, out):
    """"Split an input stream into multiple output streams, following Round
    Robin system
    """
    for outport, p in zip(cycle(out), entry.iter_packets()):
        outport.send(p)
        if entry.is_drained():
            outport.close()


@component
@outport("out", array=True, description="Replicated packets")
@inport("entry", description="Incoming packets")
def Replicate(entry, out):
    """Replicate stream of packets to multiple output streams"""
    for p in entry:
        for outport in out:
            outport.send(p.clone())
            if entry.is_drained():
                # this is here to avoid a deadlock with connections with a
                # capacity of 1:
                # - we enter the `for outport in OUT` loop above
                # - `outport.send() is called
                # - the receiver can't consume it yet because e.g. it must first
                #   receive a packet from a subsequent element port on this
                #   component
                # - the send blocks which prevents the loop through element
                #   ports (above) from progressing. Deadlock.
                # To solve it, we close element ports as soon as possible to
                # indicate to the receiver to move on, and thus allow the loop
                # here to continue.
                outport.close()
        # forked(OUT).send(p)
        p.drop()


@outport("output", array=True, description="Packets being output")
@inport("entry", description="Incoming packets")
def LoadBalance(entry, out):
    """
    Sends incoming packets to output array element with smallest backlog
    """

    active_outport = None
    substream_level = 0
    outports = load_balanced(out)
    for p in entry.iter_packets():
        if substream_level == 0:
            # find output port with the least number of downstream packets
            active_outport = outports.next_port()
        if p.get_type() == Packet.Type.OPEN:
            substream_level += 1
        elif p.get_type() == Packet.Type.CLOSE:
            substream_level -= 1
        active_outport.send(p)
