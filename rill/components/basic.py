from rill import *
from rill.fn import range
import itertools


@component
@inport("entry", description="Packets to be displayed")
@outport("out", required=False, description="Output port, if connected")
def Output(entry, out):
    """displays the content of incoming IPs open and close brackets"""
    level = 1
    for p in entry:
        if p.get_type() == Packet.Type.OPEN:
            logger.info("OPEN({})".format(level))
            level += 1
            break
        elif p.get_type() == Packet.Type.CLOSE:
            level -= 1
            logger.info("CLOSE({})".format(level))
            break
        else:
            logger.info(repr(p.get_contents()))
        out.send(p)


@component
@outport("out")
@inport("const")
def Inject(const, out):
    """Inject CONST from IIP to the IP OUT"""
    c = const.receive_once()
    if c is None:
        return
    if not out.is_closed():
        out.send(c)


@component
@outport("out", description="Single packet containing blank", type=str)
def Kick(out):
    """
    Component to generate a single packet with a single blank character

    mostly used for debugging.
    """
    out.send(" ")


@component
@inport("entry")
@outport("out")
def Passthru(entry, out):
    """Pass a stream of packets to an output stream"""
    # make it a non-looper - for testing
    p = entry.receive()
    out.send(p)


@component
@inport("entry")
@inport("count", type=int,
        description="Number of times to repeat. If None, the first packet on "
                    "IN repeats forever")
@outport("out")
def Repeat(entry, count, out):
    """Repeat each packet from IN to OUT, COUNT times"""
    count = count.receive_once()
    kwargs = {}
    if count is not None:
        kwargs['times'] = count

    # FIXME: non-loopers have some inherent problems.  for one, this component
    # is deactivated after each input packet still holding several output packets

    # p = entry.receive()
    # if p is None:
    #     return
    #
    # for i in range(count):
    #     if out.is_closed():
    #         break
    #     out.send(p.clone())

    for packet in entry.iter_packets():
        for p in itertools.repeat(packet, **kwargs):
            if out.is_closed():
                break
            out.send(p.clone())
        packet.drop()


@component
@inport("entry")
@outport("out")
def Copy(entry, out):
    """Copy all incoming packets to output"""
    for p in entry.iter_contents():
        out.send(p.clone())


@component
@inport("entry", description="Stream of packets to be discarded")
def Discard(entry):
    """Discards all incoming packets"""
    entry.receive().drop()


# @component
@inport("entry", description="Incoming stream")
@outport("out", description="Stream being passed through", required=False)
@outport("count", description="Count packet to be output", type=int)
@must_run
# def Counter(entry, out, count):
#     """Component to count a stream of packets, and output the result on the
#     count port.
#     """
#     count = 0
#     for p in entry.iter_packets():
#         count += 1
#         out.send(p)
#     count.send(count)
class Counter(Component):
    """Component to count a stream of packets, and output the result on the
    COUNT port.
    """
    def execute(self):
        # FIXME: is it possible for a looper to be terminated and then
        # reactivated? if so, self.count will be wrong.  it is designed this
        # way to allow pause/resume (i.e. fault-tolerance)
        for p in self.ports.IN.iter_packets():
            self.count += 1
            self.ports.out.send(p)
        self.ports.count.send(self.count)

    def init(self):
        self.count = 0


@component
@inport("entry", description="Packets to be sorted")
@inport("max", description="Maximum number of packets to be sorted", type=int)
@outport("out", description="Output port")
def Sort(entry, max, out):
    """
    Sort a stream of Packets to an output stream
    """
    max = max.receive_once(9999) - 1

    array = []
    for i, p in enumerate(entry.iter_packets()):
        if i == max:
            break
        array.append(p)

    # this is designed to stream results during the sort operation, which is
    # theoretically "better" than sorting everything up front with the sort()
    # function, though in practice it's likely that sort() is faster in most
    # cases by virtue of being implemented in C.
    j = 0
    k = len(array)
    n = k  # no. of packets to be sent out

    while n > 0:
        curr_min = None

        for i in range(k):
            if array[i] is not None:
                s = array[i].get_contents()
                if curr_min is None or s < curr_min:  # was `cmp(s, t) < 0`
                    j = i
                    curr_min = s
        # if (array[j] is None) break
        out.send(array[j])
        array[j] = None
        n -= 1


@component
@inport("entry", description="Packets to be sorted", type=int)
@inport("max", description="Maximum number of packets to be sorted", type=int,
        required=True)
@outport("out", description="Output port")
def Cap(entry, max, out):
    """
    Cap a numeric stream by closing IN when a value greater than or equal to
    MAX is received
    """
    max = max.receive_once()
    for p in entry.iter_packets():
        if p.get_contents() >= max:
            p.drop()
            entry.close()
            break
        else:
            out.send(p)


@inport("entry", description="Value to be captured")
class Capture(Component):
    """
    Capture a single value and store it on an internal attribute.
    Useful for testing and debugging.
    """
    def execute(self):
        captured = self.ports.IN.receive_once()
        if captured is None:
            return

        self.value = captured

    def init(self):
        self.value = None
