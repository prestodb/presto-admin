import socket
import ssl
import httplib

# Adapted from http://code.activestate.com/recipes/577548-https-httplib-client-connection-with-certificate-v/
# BSD-licensed.


class HTTPSCaCertConnection(httplib.HTTPSConnection):
    """ Class to make a HTTPS connection, with support for full client-based SSL Authentication"""

    def __init__(self, host, port, key_file, cert_file, ca_file, strict, timeout=None):
        httplib.HTTPSConnection.__init__(self, host, port, key_file, cert_file, strict, timeout)
        self.key_file = key_file
        self.cert_file = cert_file
        self.ca_file = ca_file
        self.timeout = timeout

    def connect(self):
        """ Connect to a host on a given (SSL) port.
            If ca_file is pointing somewhere, use it to check Server Certificate.

            Redefined/copied and extended from httplib.py:1105 (Python 2.6.x).
            This is needed to pass cert_reqs=ssl.CERT_REQUIRED as parameter to ssl.wrap_socket(),
            which forces SSL to check server certificate against our client certificate.
        """
        sock = socket.create_connection((self.host, self.port), self.timeout)
        if self._tunnel_host:
            self.sock = sock
            self._tunnel()
        # If there's no CA File, don't force Server Certificate Check
        if self.ca_file:
            self.sock = ssl.wrap_socket(sock, self.key_file, self.cert_file, ca_certs=self.ca_file,
                                        cert_reqs=ssl.CERT_REQUIRED)
        else:
            self.sock = ssl.wrap_socket(sock, self.key_file, self.cert_file, cert_reqs=ssl.CERT_NONE)
