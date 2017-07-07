from rill import *
from rill.fn import synced


@component
@inport("entry", description="Packets to be written", type=str)
@inport("filepath", description="File name", type=str)
@outport("out", required=False, description="Output port, if connected",
         type=str)
@must_run
def WriteLines(entry, filepath, out):
    """
    Write each packet from IN to a line filepath, and also pass it through to
    OUT.
    """
    # FIXME: consider adding a buffer to reduce IO

    filename = filepath.receive_once()
    if filename is None:
        return

    logger.info("Writing file {}".format(filename))
    try:
        with open(filename, 'w') as f:
            for p in entry:
                # long_wait_start(_timeout)

                try:
                    f.write(p.get_contents() + '\n')
                except IOError as e:
                    logger.error("Failed reading file {}: {}".format(
                        filename, str(e)))

                # long_wait_end()
                out.send(p)

    except IOError as e:
        logger.error("Failed writing file {}: {}".format(
            filename, str(e)))


@component
@inport("entry", description="Packets to be written", type=str)
@inport("filepatch", description="File name", type=str)
@outport("out", required=False, description="Output port, if connected",
         type=str)
@must_run
def Write(entry, filepath, out):
    """
    Write each packet from entry to filepath.

    Each packet is written to its own file (open/write/close), thus to avoid
    data being overwritten IN and filepath should be streams of the same length
    """
    # p = filepath.receive()
    # if p is None:
    #     return
    # filename = p.get_contents()
    #
    # p = entry.receive()
    # if p is None:
    #     return
    #
    # logger.info("Writing file {}".format(filename))
    # try:
    #     with open(filename, 'w') as f:
    #         f.write(p.get_contents())
    # except IOError as e:
    #     logger.error("Failed writing file {}: {}".format(
    #         filename, str(e)))
    # out.send(p)

    for pfile, ptext in synced(filepath, entry):
        filename = pfile.get_contents()
        pfile.drop()
        logger.info("Writing file {}".format(filename))
        try:
            with open(filename, 'w') as f:
                f.write(ptext.get_contents())
        except IOError as e:
            logger.error("Failed writing file {}: {}".format(
                filename, str(e)))
        out.send(ptext)


@component
@outport("out", description="Generated packets", type=str)
@inport("filepath", description="File name", type=str)
def ReadLines(filepath, out):
    """
    Creates a packets for each line in a file.
    """
    # filename = filepath.receive_once()
    # if filename is None:
    #     return
    #
    for filename in filepath.iter_contents():

        logger.info("Reading file {}".format(filename))
        try:
            with open(filename, 'r') as f:
                for line in f:
                    if out.is_closed():
                        break
                    out.send(line.rstrip('\n'))
        except IOError as e:
            logger.error("Failed reading file {}: {}".format(
                filename, str(e)))
