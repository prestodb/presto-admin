
def to_boolean(string):
    """
    Parses the given string into a boolean.  If its already a boolean, its 
    returned unchanged.  
    
    This method does strict parsing; only the string "True" returns the boolean 
    True, and only the string "False" returns the boolean False.  All other values
    throw a ValueError.
    
    :param string: the string to parse
    """
    if string is True or string == 'True':
        return True
    elif string is False or string == 'False':
        return False

    raise ValueError("invalid boolean string: %s" % string)

def host_string_to_parts(string, default_user=None):
    """
    Parse a string of the form [user@]host[:port] into a 3-tuple
    of (user, host, port). user and port will be None if not present.

    :param string: the string to parse
    """
    the_user = default_user
    the_host = string
    the_port = None
    if '@' in the_host:
        the_user, the_host = the_host.split('@')
    if ':' in the_host:
        the_host, the_port = the_host.split(':')
    return the_user, the_host, the_port
