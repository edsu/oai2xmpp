currents will poll an oai-pmh server and funnel new records over xmpp. 
Basic usage is:

    oai2xmpp.py http://www.doaj.org/oai.article from@example.com to@example.org

This will poll the DOAJ oai-pmh server for new records every 10 minutes and 
send any new ones over jabber from from@example.com to to@example.org.

    oai2xmpp.py --help

will give the set of options to tweak, such as the poll interval to use,
, datetime to start looking for records from (defaults to now), a set to 
limit records from, etc.

You will need to install xmppy and lxml to get this working properly:

    easy_install xmppy
    easy_install lxml
    easy_install httplib2

Then you'll need to store the password for from@jabber.org in ~/.currents Just a
plain text file with only the password will do.

Comments, questions to: Ed Summers <ehs@pobox.com>

TODO:

- pubsub (xep-0060)
- current awareness bot
- manager for polling a set of oai-pmh targets
- make the harvester more event driven (twisted?)
