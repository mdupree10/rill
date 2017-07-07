from rill import inport, outport, component

@component
@inport("entry")
@outport("out")
def Output(entry, out):
    print('In Output component\n')
    for p in entry:
        print('processing entry.p... ')
        print(repr(p.get_contents()))
        out.send(p)


@component
@inport("entry", type=str)
@inport("test", type=str)
@outport("acc", type=str)
@outport("rej", type=str)
def StartsWith(entry, test, acc, rej):
    test_str = test.receive_once()

    for p in entry.iter_packets():
        s = p.get_contents()
        if s.startswith(test_str):
            acc.send(p)
        else:
            rej.send(p)

@component
@inport("entry", type=str)
@inport("measure", type=int)
@outport("out", type=str)
def WordsToLine(entry, measure, out):
    measure = measure.receive_once()

    line = ""
    for word in entry.iter_contents():
        if measure and (len(line) + 1 + len(word)) > measure:
            out.send(line)
            # restart line
            line = word
        else:
            if line:
                line += " "
            line += word
    if line:
        # remainder
        out.send(line)

@component
@outport("out", type=str)
@inport("entry", type=str)
def LineToWords(entry, out):
    for line in entry.iter_contents():
        words = line.split()
        for word in words:
            print("Sending packet to OUT with: {}\n".format(word))
            out.send(word)
