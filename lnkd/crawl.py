from bs4 import BeautifulSoup
import config
import cookielib
import os
import re
import smtplib
import sqlite3
import string
import urllib
import urllib2

class LinkedInCrawler(object):

    def __init__(self, login, password):
        """ Start up... """
        self.login = login
        self.password = password

        # SQLite
        self.conn = sqlite3.connect('lnkd.db')
        self.conn.row_factory = sqlite3.Row
        self.createDb()

        # Simulate browser with cookies enabled
        self.cj = cookielib.MozillaCookieJar('parser.cookie.txt')
        self.opener = urllib2.build_opener(
            urllib2.HTTPRedirectHandler(),
            urllib2.HTTPHandler(debuglevel=0),
            urllib2.HTTPSHandler(debuglevel=0),
            urllib2.HTTPCookieProcessor(self.cj)
        )
        self.opener.addheaders = [
            ('User-agent', ('Mozilla/4.0 (compatible; MSIE 6.0; '
                           'Windows NT 5.2; .NET CLR 1.1.4322)'))
        ]

        self.loginPage()
        self.crawlProfiles()
        self.conn.close()


    def loadPage(self, url, data=None, retry_num=0):
        try:
            if data is not None:
                response = self.opener.open(url, data)
            else:
                response = self.opener.open(url)
            return ''.join(response.readlines())
        except:
            if retry_num < 3:
                return self.loadPage(url, data, retry_num + 1)
            else:
               raise Exception('Unable to load url "%s" after 3 tries')


    def loginPage(self):
        html = self.loadPage("https://www.linkedin.com/")
        soup = BeautifulSoup(html)
        csrf = soup.find(id="loginCsrfParam-login")['value']
        login_data = urllib.urlencode({
            'session_key': self.login,
            'session_password': self.password,
            'loginCsrfParam': csrf,
        })
        html = self.loadPage("https://www.linkedin.com/uas/login-submit", login_data)


    def loadTitle(self):
        html = self.loadPage("http://www.linkedin.com/nhome")
        soup = BeautifulSoup(html)
        return soup.find("title")


    def createDb(self):
        c = self.conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS profile
                     (profile_id real PRIMARY KEY, name text, location text, title text, company text, description text, url text, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME)''')
        self.conn.commit()
        c.close()


    def crawlProfiles(self):
        successes = 0
        changes = 0
        failures = 0
        c = self.conn.cursor()
        with open("%s/%s" % (os.path.dirname(os.path.realpath(__file__)), 'ids_to_check'), 'r') as f:
            profile_ids = f.readlines()
        for profile_id in profile_ids:
            try:
                profile_id = int(profile_id)
                url = 'https://www.linkedin.com/profile/view?id=%d' % profile_id
                html = self.loadPage(url)
                soup = BeautifulSoup(html)
                profile = {}
                profile['url'] = unicode(url, 'utf-8')
                profile['name'] = soup.select('.full-name')[0].get_text()
                current_positions = soup.select('.background-experience div.current-position')
                if len(current_positions) == 0:
                    failures += 1
                    print 'profile %d - %s: Unable to find any current positions... profile may not be viewable: %s' % (profile_id, profile['name'], url)
                else:
                    # parse current position information
                    current_position = current_positions[0]
                    profile['location'] = soup.select('#location .locality')[0].get_text()
                    profile['title'] = current_position.select('header h4')[0].get_text()
                    profile['company'] = current_position.select('header h4')[0].nextSibling()[0].get_text()
                    profile['description'] = current_position.select('.description')[0].get_text()

                    # compare to last run
                    c.execute('SELECT * FROM profile WHERE profile_id = ?', (profile_id,))
                    saved_profile = c.fetchone()
                    description_changed = saved_profile is not None and saved_profile['description'] != profile['description']

                    # e-mail if the description changed
                    if description_changed:
                        changes += 1
                        FROM = "jbwyme@gmail.com"
                        TO = ["josh@mixpanel.com"]
                        SUBJECT = "Job description changed for %s" % profile['name'].encode('utf-8')
                        TEXT = 'url: %s\n\nold:\n---------\n%s\n\nnew:\n---------\n%s' % (url, saved_profile['description'].encode('utf-8'), profile['description'].encode('utf-8'))
                        message = "From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s" % (FROM, ",".join(TO), SUBJECT, TEXT)
                        server = smtplib.SMTP('smtp.gmail.com',587) # port 465 or 587
                        server.ehlo()
                        server.starttls()
                        server.ehlo()
                        server.login(config.GMAIL_EMAIL, config.GMAIL_PASSWORD)
                        server.sendmail(FROM, TO, message)
                        server.close()

                    # save profile to db
                    if saved_profile is None or description_changed:
                        c.execute("INSERT OR IGNORE INTO profile (profile_id, name, url) VALUES (?, ?, ?)", (profile_id, profile['name'], url))
                        c.execute("UPDATE profile SET location = ?, title = ?, company = ?, description = ?, updated_at = CURRENT_TIMESTAMP WHERE profile_id = ?",
                            (profile['location'], profile['title'], profile['company'], profile['description'], profile_id,))
                        self.conn.commit()
                        print 'profile %d - %s: profile %s' % (profile_id, profile['name'], 'updated' if description_changed else 'added')

                    successes += 1
            except Exception as e:
                failures += 1
                print 'profile %d - %s: failed' % (profile_id, profile['name'])
                print "Exception: {0}".format(e)

        print 'successes: %d, changes: %d, failures %d' % (successes, changes, failures)
        c.close()

parser = LinkedInCrawler(config.LINKEDIN_EMAIL, config.LINKEDIN_PASSWORD)
