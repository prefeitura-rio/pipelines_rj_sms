import ipaddress
import requests
import socket


# [Ref] https://stackoverflow.com/a/57477670/4824627
class HostHeaderSSLAdapter(requests.adapters.HTTPAdapter):
    def resolve(self, hostname):
        # Tentamos vários servidores em busca de um que tenha o IP do site
        dns_servers = [
            "1.1.1.1",  # Cloudflare
            "8.8.8.8",  # Google
            "9.9.9.9",  # Quad9
            "208.67.222.222"  # OpenDNS
        ]
        for server in dns_servers:
            results = dns_lookup(hostname, server)
            if "A" in results:
                return results["A"][0]
        # Precisamos escolher um endereço inválido para retornar
        # 255.255.255.255 é broadcast; usa .254 que é reservado
        return "255.255.255.254"

    def send(self, request, **kwargs):
        from urllib.parse import urlparse

        connection_pool_kwargs = self.poolmanager.connection_pool_kw

        result = urlparse(request.url)
        resolved_ip = self.resolve(result.hostname)

        if result.scheme == 'https' and resolved_ip:
            request.url = request.url.replace(
                'https://' + result.hostname,
                'https://' + resolved_ip,
            )
            connection_pool_kwargs['server_hostname'] = result.hostname  # SNI
            connection_pool_kwargs['assert_hostname'] = result.hostname

            # overwrite the host header
            request.headers['Host'] = result.hostname
        else:
            # theses headers from a previous request may have been left
            connection_pool_kwargs.pop('server_hostname', None)
            connection_pool_kwargs.pop('assert_hostname', None)

        return super(HostHeaderSSLAdapter, self).send(request, **kwargs)


########################################################################
# Daqui pra baixo é um lookup de DNS simples
# [Ref] https://github.com/1ocalhost/py_cheat/blob/dd1eca597371c5dcf05f980b5cb6bb298f373cc0/dns_lookup.py

def parse_dns_string(reader, data):
    res = ''
    to_resue = None
    bytes_left = 0

    for ch in data:
        if not ch:
            break

        if to_resue is not None:
            resue_pos = chr(to_resue) + chr(ch)
            res += reader.reuse(resue_pos)
            break

        if bytes_left:
            res += chr(ch)
            bytes_left -= 1
            continue

        if (ch >> 6) == 0b11 and reader is not None:
            to_resue = ch - 0b11000000
        else:
            bytes_left = ch

        if res:
            res += '.'

    return res


class StreamReader:
    def __init__(self, data):
        self.data = data
        self.pos = 0

    def read(self, len_):
        pos = self.pos
        if pos >= len(self.data):
            raise

        res = self.data[pos: pos+len_]
        self.pos += len_
        return res

    def reuse(self, pos):
        pos = int.from_bytes(pos.encode(), 'big')
        return parse_dns_string(None, self.data[pos:])


def make_dns_query_domain(domain):
    def f(s):
        return chr(len(s)) + s

    parts = domain.split('.')
    parts = list(map(f, parts))
    return ''.join(parts).encode()


def make_dns_request_data(dns_query: bytes) -> bytes:
    req = b'\xaa\xbb\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00'
    req += dns_query
    req += b'\x00\x00\x01\x00\x01'
    return req


def add_record_to_result(result, type_, data, reader):
    if type_ == 'A':
        item = str(ipaddress.IPv4Address(data))
    elif type_ == 'CNAME':
        item = parse_dns_string(reader, data)
    else:
        return

    result.setdefault(type_, []).append(item)


def parse_dns_response(res, dq_len, req):
    reader = StreamReader(res)

    def get_query(s):
        return s[12:12+dq_len]

    data = reader.read(len(req))
    assert(get_query(data) == get_query(req))

    def to_int(bytes_):
        return int.from_bytes(bytes_, 'big')

    result = {}
    res_num = to_int(data[6:8])
    for i in range(res_num):
        reader.read(2)
        type_num = to_int(reader.read(2))

        type_ = None
        if type_num == 1:
            type_ = 'A'
        elif type_num == 5:
            type_ = 'CNAME'

        reader.read(6)
        data = reader.read(2)
        data = reader.read(to_int(data))
        add_record_to_result(result, type_, data, reader)

    return result


def dns_lookup(domain: str, address: str):
    dns_query = make_dns_query_domain(domain)
    dq_len = len(dns_query)

    req = make_dns_request_data(dns_query)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(2)

    try:
        sock.sendto(req, (address, 53))
        res, _ = sock.recvfrom(1024 * 4)
        result = parse_dns_response(res, dq_len, req)
    except Exception:
        return
    finally:
        sock.close()

    return result
