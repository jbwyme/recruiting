from bs4 import BeautifulSoup
import config
import cookielib
import os
import random
import re
import smtplib
import sqlite3
import string
import time
import urllib
import urllib2

class LinkedInCrawler(object):

    def __init__(self, login, password):
        self.login = login
        self.password = password
        self.profile_ids = []
        self.profile_creds = []
        if os.path.isfile('profiles'):
            with open('profiles', 'r') as f:
                print "loading existing profiles..."
                for line in f.readlines():
                    profile_id = re.search('id=([0-9]+)', line).group(1)
                    cred = line.strip()
                    self.profile_ids.append(profile_id)
                    self.profile_creds.append(cred)

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
            ('User-agent', ('Mozilla/5.0 (Macintosh; '
                           'Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.111 Safari/537.36)'))
        ]

        self.loginPage()
        self.findProfiles()
        #self.crawlProfiles()
        self.conn.close()


    def loadPage(self, url, data=None, retry_num=0):
        try:
            if data is not None:
                response = self.opener.open(url, data)
            else:
                response = self.opener.open(url)
            return ''.join(response.readlines())
        except Exception as e:
            if retry_num < 3:
                print 'retrying loadPage...'
                return self.loadPage(url, data, retry_num + 1)
            else:
                print 'Unable to load url "%s" after 3 tries' % url
                raise


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
        soup = BeautifulSoup(html)
        if soup.find(id='verification-code') is not None:
            post_data = {}
            form = soup.find('form')
            for hi in form.select('input[type=hidden]'):
                post_data[hi['name']] = hi['value']
            code = raw_input("Please enter your verification code: ")
            post_data['PinVerificationForm_pinParam'] = code
            verification_data = urllib.urlencode(post_data)
            html = self.loadPage("https://www.linkedin.com/uas/ato-pin-challenge-submit", verification_data)

    def createDb(self):
        c = self.conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS profile
                     (profile_id real PRIMARY KEY, name text, location text, title text, company text, description text, url text, created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME)''')
        self.conn.commit()
        c.close()

    def findProfiles(self, url=None):
        url = url or 'https://www.linkedin.com/profile/view?id=39163401&authType=name&authToken=GoOL&offset=5&trk=prof-sb-pdm-similar-photo'
        profile_id = int(re.search('id=([0-9]+)', url).group(1))
        html = self.loadPage(url)
        self._saveProfile(profile_id, url, html)
        soup = BeautifulSoup(html)
        similar_urls = [a['href'] for a in soup.find(class_='discovery-results').findAll('a')]
        for surl in similar_urls:
            if '/profile/view' in surl:
                profile_id = re.search('id=([0-9]+)', surl).group(1)
                authType = ''
                authToken = ''
                try:
                    authType = re.search('authType=([a-zA-Z_0-9]+)', surl).group(1)
                    authToken = re.search('authToken=([a-zA-Z_0-9]+)', surl).group(1)
                except:
                    pass
                if profile_id not in self.profile_ids:
                    cred = 'id=%s&authType=%s&authToken=%s' % (profile_id, authType, authToken)
                    self.profile_ids.append(profile_id)
                    self.profile_creds.append(cred)
                    with open('profiles', 'a+') as f:
                        f.write(cred + '\n')
                    print 'found url: %s' % cred
        self._delay()
        for surl in similar_urls:
            self.findProfiles(surl)

    def crawlProfiles(self):
        successes = 0
        failures = 0
        for cred in self.profile_creds:
            try:
                profile_id = int(re.search('id=([0-9]+)', cred).group(1))
                url = 'https://www.linkedin.com/profile/view?%s' % cred
                html = self.loadPage(url)
                self._saveProfile(profile_id, url, html)
                successes += 1
            except Exception as e:
                import traceback
                failures += 1
                print 'profile %d - %s: failed' % (profile_id, profile['name'] if 'name' in profile else 'unknown')
                print "Exception: %s, Trace: %s" % (e, traceback.format_exc())

            self._delay()

        print 'successes: %d, changes: %d, failures %d' % (successes, changes, failures)

    def _saveProfile(self, profile_id, url, html):
        soup = BeautifulSoup(html)
        c = self.conn.cursor()
        profile = {}
        profile['url'] = unicode(url, 'utf-8')
        profile['name'] = soup.select('.full-name')[0].get_text()
        current_positions = soup.select('.background-experience div.current-position')
        if len(current_positions) == 0:
            print 'profile %d - %s: Unable to find any current positions... profile may not be viewable: %s' % (profile_id, profile['name'], url)
        else:
            # parse current position information
            current_position = current_positions[0]
            profile['location'] = soup.select('#location .locality')[0].get_text()
            profile['title'] = current_position.select('header h4')[0].get_text()
            profile['company'] = current_position.select('header h4')[0].nextSibling()[0].get_text()
            description_el = current_position.select('.description')
            if len(description_el) == 0:
                # no description provided for the current position
                profile['description'] = ''
            else:
                profile['description'] = current_position.select('.description')[0].get_text()

        # compare to last run
        c.execute('SELECT * FROM profile WHERE profile_id = ?', (profile_id,))
        saved_profile = c.fetchone()
        description_changed = saved_profile is not None and saved_profile['description'] != profile['description']

        # e-mail if the description changed
        if description_changed:
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
        c.close()


    def _delay(self):
        delay = random.randrange(5,30,1)
        print 'waiting %d seconds...' % delay
        time.sleep(delay) # random delay

parser = LinkedInCrawler(config.LINKEDIN_EMAIL, config.LINKEDIN_PASSWORD)
