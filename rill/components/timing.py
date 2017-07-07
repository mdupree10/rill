
import time
from rill import *


@component
@outport("out")
@inport("interval", type=float)
def Heartbeat(interval, out):
    """Generates a packet every 'n' seconds"""
    # receive interval in seconds
    itvl = interval.receive_once()
    if itvl is None:
        return

    # when the send returns False, component closes down
    while True:
        if not out.send(" "):
            break
        time.sleep(itvl)


@component
@outport("out")
@inport("in")
@inport("delay", type=float, required=True, static=True)
def SlowPass(entry, delay, out):
    """
    Pass a stream of packets to an output stream with a delay between packets
    """
    # FIXME: this has to be disabled for the pickle/resume test to succeed. make test more robust
    # time.sleep(delay)
    for p in entry:
        # in order to remain fault-tolerant, we have to sleep after we send
        # and not before, or else we could be terminated while holding a
        # packet.
        out.send(p)
        time.sleep(delay)


# FIXME: I think this can be replaced with a Copy using the NULL input port

@component
@outport("out")
@inport("in")
@inport("trigger")
def Gate(entry, trigger, out):
    """Copies incoming packets - delayed until trigger received"""
    # receive trigger
    tp = trigger.receive_once()
    if tp is None:
        return

    logger.info("got trigger")

    rp = entry.receive()
    logger.info("rp = '" + rp + "'")

    if rp is None:
        return
    # entry.close()

    # pass output
    out.send(rp.get_contents())
    rp.drop()
