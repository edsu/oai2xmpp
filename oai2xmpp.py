#!/usr/bin/env python

from time import sleep
from datetime import datetime
from urllib import urlencode
from optparse import OptionParser

import anydbm

from lxml import etree
import xmpp
import httplib2

ns = {
        'oai': 'http://www.openarchives.org/OAI/2.0/',
        'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/',
        'dc': 'http://purl.org/dc/elements/1.1/',
     }

default_poll_interval = 60 * 10 # seconds
user_agent = 'oai2xmpp - http://github.com/edsu/currents/'

seen = anydbm.open('oai2xmpp.db', 'c')

def get_password():
    return file('/home/ed/.oai2xmpp').read().strip()

def list_records(base_url, from_dt=None, rtoken=None, set=set):
    # determine appropriate request url 
    fmt = datetime_format(granularity(identify(base_url)))
    if rtoken:
        q = {'verb': 'ListRecords', 'resumptionToken': rtoken}
    else:
        dt = from_dt.strftime(fmt)
        q = {'verb': 'ListRecords', 'from': dt, 'metadataPrefix': 'oai_dc'}
        if set:
            q['set'] = set
    url = base_url + '?' + urlencode(q)

    # handle 503 errors asking us to hold our horses
    doc = None
    while doc == None:
        try:
            print "fetching: %s" % url
            h = httplib2.Http()
            headers = {'User-Agent': user_agent}
            response, content = h.request(url, headers=headers) 
            if response.status == 503:
                wait_seconds = int(response['retry-after'])
                print "server told us to wait %s seconds" % wait_seconds
                sleep(wait_seconds)
            else:
                doc = etree.fromstring(content)
        except IOError, e:
            print "IOError when fetching records: %s" % e
            raise StopIteration

    for r in _all(doc, '/oai:OAI-PMH/oai:ListRecords/oai:record'):
        yield r

    # recurse with resumption token if one is present
    e = doc.xpath('/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken', 
                  namespaces=ns)
    rtoken = resumption_token(doc)
    if rtoken:
        for r in list_records(base_url, rtoken=rtoken):
            yield r

def jabber_client(from_jid):
    pwd = get_password()
    jid = xmpp.protocol.JID(from_jid)
    client = xmpp.Client(jid.getDomain(), debug=[])
    client.connect()
    client.auth(jid.getNode(), pwd)
    return client

def record_identifier(r):
    return _first(r, './/oai:header/oai:identifier')

def record_datestamp(r):
    return _first(r, './/oai:header/oai:datestamp')

def metadata_identifiers(r):
    return [e.text for e in _all(r, './/oai:metadata/oai_dc:dc/dc:identifier')]

def resumption_token(d):
    return _first(d, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken')

def identify(base_url):
    return etree.parse(base_url + '?verb=Identify')

def granularity(doc):
    return _first(doc, '/oai:OAI-PMH/oai:Identify/oai:granularity')

def datetime_format(granularity):
    if granularity == 'YYYY-MM-DD':
        return '%Y-%m-%d'
    elif granularity == 'YYYY-MM-DDThh:mm:ssZ':
        return '%Y-%m-%dT%H:%M:%SZ'
    else:
        raise RuntimeError("invalid granularity %s" % granularity)

def record_summary(r):
    identifier = record_identifier(r)
    datestamp = record_datestamp(r)
    msg = "%s [%s] " % (identifier, datestamp)
    msg += ', '.join(metadata_identifiers(r))
    return msg

def poll(base_url, from_jid, to_jid, from_dt, poll_interval, set=None):
    print "polling: %s" % base_url
    print "from: %s" % from_jid
    print "to: %s" % to_jid
    print "from_datetime: %s" % from_dt
    print "poll interval: %s" % poll_interval
    if set:
        print "set: %s" % set

    while True:
        print "sleeping for %s" % poll_interval
        sleep(poll_interval)
        client = jabber_client(from_jid)
        now = datetime.utcnow()
        print "checking for new records since %s" % from_dt 
        for record in list_records(base_url, from_dt, set=set):
            id = record_identifier(record)
            if seen.has_key(id):
                print "already seen %s" % id
                continue
            print "found new record: %s" % record_summary(record)
            client.send(xmpp.protocol.Message(to_jid, etree.tostring(record)))
            seen[id] = etree.tostring(record)
            sleep(1) # not to overwhelm jabber server :(
        from_dt = now 
        client.disconnect()

def _first(doc, xpath):
    e = doc.xpath(xpath, namespaces=ns)
    if len(e) > 0:
        return e[0].text
    return None

def _all(doc, xpath):
    e = doc.xpath(xpath, namespaces=ns)
    return e

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-p', '--pollinterval', dest='poll_interval', 
                      help='seconds to wait between ListRecords requests ' + \
                           'defaults to %s seconds' % default_poll_interval,
                      type=int,
                      default=default_poll_interval)
    parser.add_option('-f', '--fromdt', dest='from_dt', 
                      help='the default datetime to start collecting ' + \
                            'records from, defaults to current datetime ' + \
                            'UTC, you must supply datetime in ISO 8601 format')
    parser.add_option('-s', '--set', dest='set',
                      help='set a setSpec to harvest from')

    (opts, args) = parser.parse_args()

    if len(args) != 3:
        parser.error("must supply oai-pmh base url, a jabber id to send " + \
                     "updates from, and a jabber id to send updates to") 
    base_url, from_jid, to_jid = args[0:3]

    if opts.from_dt:
        from_dt = datetime.strptime('%Y-%m-%dT%H:%M:%SZ', opts.from_dt)
    else:
        from_dt = datetime.utcnow()

    poll(base_url, from_jid, to_jid, from_dt, opts.poll_interval, set=opts.set)
