#!/usr/bin/env python
try:
    import json
except ImportError:
    import simplejson as json
import redis

def dump(fp, host='localhost', port=6379, password=None, db=0, pretty=False, key='*'):
    r = redis.Redis(host=host, port=port, password=password, db=db, decode_responses=True)
    kwargs = {}
    if not pretty:
        kwargs['separators'] = (',', ':')
    else:
        kwargs['indent'] = 2
        kwargs['sort_keys'] = True
    encoder = json.JSONEncoder(**kwargs)
    for key, type, value in _reader(r, pretty, key):
        d = {}
        d[key]={'type':type,'value':value}
        item = encoder.encode(d)
        fp.write(item)
        fp.write("\n")

def _reader(r, pretty, prefixkey):
    print("prefixkey is {}".format(prefixkey))
    for key in r.keys(prefixkey):
        try:
            type = r.type(key)
            if type == 'string':
                value = r.get(key)
            elif type == 'list':
                value = r.lrange(key, 0, -1)
            elif type == 'set':
                value = list(r.smembers(key))
                if pretty:
                    value.sort()
            elif type == 'zset':
                value = r.zrange(key, 0, -1, False, True)
            elif type == 'hash':
                value = r.hgetall(key)
            else:
                raise Exception("Unknown key type: %s".format(type))
            # yield bytes.decode(key), bytes.decode(type), bytes.decode(value)
            yield key, type, value
        except Exception as e:
            print('skip ...{}'.format(key))

if __name__ == '__main__':
    import optparse
    import sys


    def options_to_kwargs(options):
        args = {}
        if options.host:
            args['host'] = options.host
        if options.port:
            args['port'] = int(options.port)
        if options.password:
            args['password'] = options.password
        if options.db:
            args['db'] = int(options.db)
        if options.key:
            args['key'] = options.key
        # dump only
        if hasattr(options, 'pretty') and options.pretty:
            args['pretty'] = True
        # load only
        if hasattr(options, 'empty') and options.empty:
            args['empty'] = True
        return args
    
    def do_dump(options):
        if options.output:
            output = open(options.output, 'w')
        else:
            output = sys.stdout
        
        kwargs = options_to_kwargs(options)
        dump(output, **kwargs)
        
        if options.output:
            print("close file...")
            output.close()


    usage = "Usage: %prog [options]"
    usage += "\n\nDump data from specified or default redis."
    usage += "\n\nIf no output file is specified, dump to standard output."

    parser = optparse.OptionParser(usage=usage)
    parser.add_option('-H', '--host', help='connect to HOST (default localhost)')
    parser.add_option('-p', '--port', help='connect to PORT (default 6379)')
    #parser.add_option('-s', '--socket', help='connect to SOCKET')
    parser.add_option('-w', '--password', help='connect with PASSWORD')
    parser.add_option('-d', '--db', help='dump DATABASE (0-N, default 0)')
    parser.add_option('-o', '--output', help='write to OUTPUT instead of stdout')
    parser.add_option('-k', '--key', help='prefix key')

    options, args = parser.parse_args()
    if len(args) > 0:
        parser.print_help()
        exit(4)
    do_dump(options)

