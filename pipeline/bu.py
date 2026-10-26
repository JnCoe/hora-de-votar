"""Read the Presidente comparecimento from a boletim de urna (bu.dat or busa.dat).

Both files are ASN.1 DER: an envelope whose OCTET STRING holds the boletim.
Inside it, each election has a SEQUENCE starting with the eleição code
(INTEGER 6258 here), followed by integers (the first is aptos), and then a
SEQUENCE of per-cargo results, each one starting with ENUMERATED cargo
(1 = Presidente) and INTEGER comparecimento.
"""

from config import ELEICAO

SEQ, INT, ENUM, OCTETS = 0x30, 0x02, 0x0A, 0x04


class NotDER(ValueError):
    pass


def _items(b: bytes, i: int, end: int):
    """Yield (tag, value_start, value_end) of the DER items in b[i:end]."""
    while i < end:
        if end - i < 2:
            raise NotDER
        tag = b[i]
        i += 1
        if tag & 0x1F == 0x1F:  # multi-byte tag number
            while i < end and b[i] & 0x80:
                i += 1
            i += 1
        if i >= end:
            raise NotDER
        n = b[i]
        i += 1
        if n & 0x80:
            k = n & 0x7F
            n = int.from_bytes(b[i:i + k], "big")
            i += k
        if i + n > end:
            raise NotDER
        yield tag, i, i + n
        i += n


def _int(b: bytes, s: int, e: int) -> int:
    return int.from_bytes(b[s:e], "big", signed=True)


def _find_election(b: bytes, s: int, e: int, code: int):
    """Depth-first search for the election SEQUENCE; returns (aptos, comparecimento)."""
    for tag, vs, ve in _items(b, s, e):
        if tag == SEQ:
            kids = list(_items(b, vs, ve))
            if (len(kids) >= 2 and kids[0][0] == INT and _int(b, *kids[0][1:]) == code
                    and kids[1][0] == INT):
                aptos = _int(b, *kids[1][1:])
                for ktag, ks, ke in kids:
                    if ktag != SEQ:
                        continue
                    for ctag, cs, ce in _items(b, ks, ke):
                        cargo = list(_items(b, cs, ce))
                        if (ctag == SEQ and len(cargo) >= 2 and cargo[0][0] == ENUM
                                and _int(b, *cargo[0][1:]) == 1 and cargo[1][0] == INT):
                            return aptos, _int(b, *cargo[1][1:])
            found = _find_election(b, vs, ve, code)
            if found:
                return found
        elif tag == OCTETS and ve > vs and b[vs] == SEQ:  # maybe the embedded boletim
            try:
                found = _find_election(b, vs, ve, code)
            except NotDER:  # binary data (e.g. a hash) that merely starts with 0x30
                continue
            if found:
                return found
    return None


def presidente(bu: bytes, code: int = int(ELEICAO)) -> tuple[int, int] | None:
    """(aptos, comparecimento) for Presidente in this election, or None if absent."""
    return _find_election(bu, 0, len(bu), code)
