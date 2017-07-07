from rill import *


@component
@outport("out", type=str)
@inport("entry", type=str)
@inport("pre", type=str, required=True)
def Prefix(entry, pre, out):
    """
    Prefix each packet IN with the given PRE and copy it to OUT
    """
    prefix = pre.receive_once()

    for p in entry:
        text = prefix + p.get_contents()
        p.drop()
        out.send(text)


@component
@outport("out", type=str)
@inport("entry", type=str)
@inport("pre", type=str, required=True)
@inport("post", type=str, required=True)
def Affix(entry, pre, post, out):
    """
    For each packet IN add the Strings PRE as a prefix and POST as a suffix,
    and copy to OUT
    """
    spre = pre.receive_once()
    spost = post.receive_once()

    for s in entry.iter_contents():
        sout = spre + s + spost
        out.send(sout)


@component
@outport("out", type=str)
@inport("entry", type=str)
def DedupeSuccessive(entry, out):
    """
    Take text IN and only send OUT where it differs from the previous
    text
    """
    previous = ""
    for s in entry.iter_contents():
        if not previous == s:
            out.send(s)
        previous = s



# @component
# @ComponentDescription(
#     "Pass through a CSV stream, also output LIMITS of field lengths as CSV")
# @outport("out")
# @outport("limits")
# @inport("entry")
# @inport("sep")
# def FieldLimits(entry, out):
#     # Default separator
#     sep = ","
#
#     # Get separator from SEP IIP
#     psep = sepport.receive()
#     if (psep is not None):
#         sepport.close()
#         sep = psep.get_contents()
#         self.drop(psep)
#
#     # IN === entry
#     Pass through entry to out, keeping greatest field lengths
#     int[]
#     nlimits = None
#     p
#     for p in entry:
#         o = p.get_contents()
#
#         # Get fields for self record
#         fields = o.split(sep)
#
#         # Prepare limits data
#         if (nlimits is None):
#             nlimits = int[fields.length]
#         # Remember greatest field length
#         for (i = 0 i < nlimits.length i += 1):
#             if (fields[i].length() > nlimits[i]):
#                 nlimits[i] = fields[i].length()
#         # Pass through
#         outport.send(p)
#     if (nlimits is not None):
#         # Send LIMITS as single CSV record
#         slimits = ""
#         for (j = 0 j < nlimits.length j += 1):
#             slimits = slimits + nlimits[j]
#             if (j < nlimits.length - 1):
#                 slimits += sep
#         plimits = self.create(slimits)
#         # limitport.send(plimits)


@component
@outport("out", type=str)
@inport("entry", type=str)
def LineToWords(entry, out):
    """Take space-separated words in a record IN and deliver individual words
    OUT"""
    for line in entry.iter_contents():
        words = line.split()
        for word in words:
            out.send(word)


@component
@outport("out", type=str)
@inport("entry", type=str)
def LowerCase(entry, out):
    """Convert text entry to lower case and send OUT"""
    for s in entry.iter_contents():
        lower = s.lower()
        out.send(lower)


# @component
# @ComponentDescription(
#     "Replace characters apart from EXC in each packet IN with the given OBS and copy to OUT")
# @outport("OUT")
# @inport("IN")
# @inport("OBS")
# @inport("EXC")
# def Obscure(IN, OUT):
#     char
#     obs = ' '  # Default obscure with space
#     pobs = obsport.receive()
#     if (pobs is not None):
#         obs = (pobs.get_contents()).char_at(0)
#         self.drop(pobs)
#     obsport.close()
#
#     exc = " "  # Default do not obscure spaces
#     pexc = excport.receive()
#     if (pexc is not None):
#         exc = pexc.get_contents()
#         self.drop(pexc)
#     excport.close()
#
#     for pin in IN:
#         out = ""
#         in = pin.get_contents()
#         if ( in is not None):
#             in += exc  # add one EXC token as a marker
#             e = in.index_of(exc)
#             s = 0
#             while ( in.length() > out.length() and e > -1):
#                 out += get_string_filled_with((e - s), obs) + exc
#                 # logger.info("in:  |" + in + "|\nout: |" + out + "|")
#                 s = e + exc.length()
#                 e = in.index_of(exc, s)
#             # self.drop the marker
#             out = out.substring(0, out.length() - exc.length())
#             # logger.info("out: |" + out + "|")
#         pin.drop()  # did you hear that?
#
#         pout = self.create(out)
#         outport.send(pout)
#
#
# def get_string_filled_with(number, char filler
#
# ):
# filled = ""
# if (number > 0):
#     char[]
#     fillers = char[number]
#     for (n = 0 n < fillers.length n += 1):
#         fillers[n] = filler
#     filled = String(fillers)
# return filled
#
# """Pad fields to given length in a stream of character-separated records
# """
#
#
# @component
# @ComponentDescription(
#     "Pass through a character SEParated stream, adding PAD up to LIMITS of field lengths")
# @outport("OUT")
# @inport("IN")
# @inport("LIMITS")
# @inport("PAD")
# @inport("SEP")
# def PadFields(Component):
#     # Default separator
#     sep = ","
#
#     # Get separator from SEP IIP
#     psep = sepport.receive()
#     if (psep is not None):
#         sepport.close()
#         sep = psep.get_contents()
#         self.drop(psep)
#
#     # Default pad
#     pad = " "
#
#     # get pad from PAD IIP
#     ppad = padport.receive()
#     if (ppad is not None):
#         padport.close()
#         pad = ppad.get_contents()
#         self.drop(ppad)
#
#     # get LIMITS
#     int[]
#     nlimits = None
#     plimit = limitport.receive()
#     if (plimit is not None):
#         limitport.close()
#         String[]
#         slimits = (plimit.get_contents()).split(sep)
#         nlimits = int[slimits.length]
#         for (j = 0 j < nlimits.length j += 1):
#             nlimits[j] = 0
#             try:
#                 nlimits[j] = Integer.parse_int(slimits[j])
#             except NumberFormatException as e:
#                 e.print_stack_trace()
#         self.drop(plimit)
#
#     # Pass through IN to OUT, PADding fields to LIMITS
#     for pin in IN:
#         in = pin.get_contents()
#
#         # Get fields for self record
#         fields = in.split(sep)
#
#         # Pad each field to limit
#         for (i = 0 i < nlimits.length i += 1):
#             while (fields[i].length() < nlimits[i]):
#                 fields[i] += pad
#         spadded = ""
#         for (k = 0 k < fields.length k += 1):
#             spadded += fields[k]
#             if (k < fields.length - 1):
#                 spadded += sep
#
#         # Pass through
#         pout = self.create(spadded)
#         outport.send(pout)
#         pin.drop()


@component
@outport("out", type=str)
@inport("entry", description="Strings to have replacement applied", type=str)
@inport("regex", description="regular expression",
        type=str, required=True)
@inport("repl", description="Replacement String", type=str, required=True)
def ReplaceRegExp(entry, regex, repl, out):
    """
    Replace all occurrences of FIND in each packet IN with the given
    REPL and copy to OUT
    """
    find = regex.compile(regex.receive_once())
    repl = repl.receive_once()

    for s in entry.iter_contents():
        out = find.sub(s, repl)
        out.send(out)


@component
@outport("out")
@inport("entry", description="Strings to be modified", type=str)
@inport("find", description="Search target", type=str, required=True)
@inport("repl", description="Replacement text", type=str, required=True)
def ReplaceString(entry, find, repl, out):
    """
    Replace all occurrences of text matching FIND (case-sensitive) in each
    packet IN with the given REPL and send to OUT
    """
    find = find.receive_once()
    repl = repl.receive()

    for s in entry.iter_contents():
        out = s.replace(find, repl)
        out.send(out)


@component
@outport("out", type=str)
@inport("entry", type=str)
@inport("measure", type=int)
def WordsToLine(entry, measure, out):
    """
    Take words IN and deliver OUT a line no longer than MEASURE characters
    """
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
def ConcatStr(entry, out):
    """
    Concatenate all packets from IN into one string sent to OUT
    """
    result = ""
    for s in entry.iter_contents():
        result += s
    out.send(result)


@component
@outport("acc", type=str)
@outport("rej", type=str)
@inport("in", type=str)
@inport("test", type=str)
def StartsWith(entry, test, acc, rej):
    """
    Route packets starting with TEST to ACC, others to REJ
    """
    test_str = test.receive_once()

    for p in entry.iter_packets():
        s = p.get_contents()
        if s.startswith(test_str):
            acc.send(p)
        else:
            rej.send(p)
